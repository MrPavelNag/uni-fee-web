// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {IERC20Metadata} from "@openzeppelin/contracts/token/ERC20/extensions/IERC20Metadata.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";
import {Ownable2Step} from "@openzeppelin/contracts/access/Ownable2Step.sol";
import {ReentrancyGuard} from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import {Pausable} from "@openzeppelin/contracts/utils/Pausable.sol";
import {IChainlinkAggregatorV3} from "./interfaces/IChainlinkAggregatorV3.sol";

interface IWETHLike {
    function withdraw(uint256 amount) external;
}

/**
 * @title RIKOVault
 * @notice Custody vault with whitelist-gated deposits and ERC20 share token (RIKO).
 *
 * Core model:
 * - user deposits whitelisted token
 * - vault mints RIKO equal to token USD value
 * - user redeems RIKO to receive chosen whitelisted token by current USD oracle quote
 *
 * Security controls:
 * - whitelist-only assets
 * - Chainlink stale-check and non-negative checks
 * - nonReentrant + pause + CEI + SafeERC20
 */
contract RIKOVault is ERC20, Ownable2Step, ReentrancyGuard, Pausable {
    using SafeERC20 for IERC20Metadata;

    /*//////////////////////////////////////////////////////////////
                                CONSTANTS
    //////////////////////////////////////////////////////////////*/

    uint8 private constant MIN_DECIMALS = 1;
    uint8 private constant MAX_DECIMALS = 18;
    uint8 private constant USD_DECIMALS = 6;
    uint8 private constant RIKO_DECIMALS = 6;
    string public constant CONTRACT_VERSION = "v1.1";

    /*//////////////////////////////////////////////////////////////
                                  ERRORS
    //////////////////////////////////////////////////////////////*/

    error RV_UnsupportedToken();
    error RV_InvalidAmount();
    error RV_InvalidReceiver();
    error RV_InvalidOracle();
    error RV_OracleFeedMismatch();
    error RV_OraclePriceInvalid();
    error RV_OraclePriceStale();
    error RV_OracleDecimalsInvalid();
    error RV_TokenDecimalsInvalid();
    error RV_SlippageExceeded();
    error RV_InsufficientLiquidity();
    error RV_GlobalCapExceeded();
    error RV_TokenCapExceeded();
    error RV_InvalidCustodyAddress();
    error RV_InvalidRikoPrice();
    error RV_PendingRedemptionExists();
    error RV_PendingRedemptionNotFound();
    error RV_InvalidPendingRedemptionOperator();
    error RV_PendingRedemptionOperatorOnly();
    error RV_NativeTransferFailed();

    /*//////////////////////////////////////////////////////////////
                                   TYPES
    //////////////////////////////////////////////////////////////*/

    struct TokenConfig {
        bool allowed;
        address oracle;
        uint32 maxOracleAge;
        bytes32 expectedFeedDescriptionHash;
        uint8 tokenDecimals;
    }

    struct PendingRedemption {
        uint256 rikoLocked;
        uint256 tokenOut;
        uint256 minTokenOut;
        address receiver;
    }

    /*//////////////////////////////////////////////////////////////
                                  STORAGE
    //////////////////////////////////////////////////////////////*/

    // 1 RIKO = 1 USD with 6 decimals (USDT-style UX).
    uint256 public constant RIKO_DECIMALS_SCALE = 1e6;

    mapping(address => TokenConfig) public tokenConfigs;
    // 0 means "no cap".
    uint256 public globalSupplyCapUsd6;
    mapping(address => uint256) public tokenTvlCapUsd6;
    mapping(address => bool) public keepOnVaultByToken;
    address public custodyAddress;
    uint256 public rikoPriceUsd6;
    mapping(address => mapping(address => PendingRedemption[])) private _pendingRedemptionQueue;
    mapping(address => mapping(address => uint256)) private _pendingRedemptionHead;
    address public pendingRedemptionOperator;
    address public wrappedNativeToken;

    /*//////////////////////////////////////////////////////////////
                                   EVENTS
    //////////////////////////////////////////////////////////////*/

    event TokenConfigUpdated(
        address indexed token,
        bool allowed,
        address indexed oracle,
        uint32 maxOracleAge,
        bytes32 expectedFeedDescriptionHash
    );
    event Deposited(address indexed user, address indexed token, uint256 tokenIn, uint256 rikoMinted);
    event Redeemed(address indexed user, address indexed token, uint256 rikoBurned, uint256 tokenOut);
    event GlobalSupplyCapUpdated(uint256 capUsd6);
    event TokenTvlCapUpdated(address indexed token, uint256 capUsd6);
    event TokenDepositStorageModeUpdated(address indexed token, bool keepOnVault);
    event CustodyAddressUpdated(address indexed custodyAddress);
    event RikoPriceUpdated(uint256 rikoPriceUsd6);
    event PendingRedemptionOperatorUpdated(address indexed operator);
    event WrappedNativeTokenUpdated(address indexed wrappedNativeToken);
    event RedeemQueued(
        address indexed account,
        address indexed token,
        address indexed receiver,
        uint256 rikoLocked,
        uint256 tokenOut,
        uint256 minTokenOut
    );
    event RedeemCompleted(
        address indexed account, address indexed token, address indexed receiver, uint256 rikoBurned, uint256 tokenOut
    );
    event RedeemCancelled(address indexed account, address indexed token, uint256 rikoReturned);

    /*//////////////////////////////////////////////////////////////
                                 LIFECYCLE
    //////////////////////////////////////////////////////////////*/

    constructor(address admin) ERC20("RIKO", "RIKO") Ownable(admin) {
        if (admin == address(0)) revert RV_InvalidReceiver();
        custodyAddress = admin;
        rikoPriceUsd6 = RIKO_DECIMALS_SCALE;
        wrappedNativeToken = _defaultWrappedNativeToken(block.chainid);
        emit CustodyAddressUpdated(admin);
        emit RikoPriceUpdated(rikoPriceUsd6);
        emit WrappedNativeTokenUpdated(wrappedNativeToken);
    }

    /*//////////////////////////////////////////////////////////////
                             ADMIN FUNCTIONS
    //////////////////////////////////////////////////////////////*/

    function decimals() public pure override returns (uint8) {
        return RIKO_DECIMALS;
    }

    /// @notice Pause state-changing user actions.
    function pause() external onlyOwner {
        _pause();
    }

    /// @notice Unpause state-changing user actions.
    function unpause() external onlyOwner {
        _unpause();
    }

    /// @notice Set global TVL cap in USD6 for total minted RIKO supply.
    /// @param capUsd6 Cap value in 6-decimal USD units. Zero disables the cap.
    function setGlobalSupplyCapUsd6(uint256 capUsd6) external onlyOwner {
        globalSupplyCapUsd6 = capUsd6;
        emit GlobalSupplyCapUpdated(capUsd6);
    }

    /// @notice Set per-token TVL cap in USD6 based on vault + custody exposure.
    /// @param token Whitelisted token address.
    /// @param capUsd6 Cap value in 6-decimal USD units. Zero disables the cap.
    function setTokenTvlCapUsd6(address token, uint256 capUsd6) external onlyOwner {
        if (token == address(0)) revert RV_UnsupportedToken();
        tokenTvlCapUsd6[token] = capUsd6;
        emit TokenTvlCapUpdated(token, capUsd6);
    }

    /// @notice Set deposit storage mode for a specific token.
    /// @dev When enabled, newly deposited funds remain on vault and are not forwarded to custody.
    /// @param token Token address.
    /// @param keepOnVault True to keep deposits on vault, false to forward to custody.
    function setTokenDepositStorageMode(address token, bool keepOnVault) external onlyOwner {
        if (token == address(0)) revert RV_UnsupportedToken();
        keepOnVaultByToken[token] = keepOnVault;
        emit TokenDepositStorageModeUpdated(token, keepOnVault);
    }

    /// @notice Set custody address where incoming deposits are forwarded.
    /// @param newCustodyAddress New custody wallet address.
    function setCustodyAddress(address newCustodyAddress) external onlyOwner {
        if (newCustodyAddress == address(0)) revert RV_InvalidCustodyAddress();
        custodyAddress = newCustodyAddress;
        emit CustodyAddressUpdated(newCustodyAddress);
    }

    /// @notice Set configurable RIKO price in USD6 for mint/redeem conversions.
    /// @param newRikoPriceUsd6 New price in 6-decimal USD units.
    function setRikoPriceUsd6(uint256 newRikoPriceUsd6) external onlyOwner {
        if (newRikoPriceUsd6 == 0) revert RV_InvalidRikoPrice();
        rikoPriceUsd6 = newRikoPriceUsd6;
        emit RikoPriceUpdated(newRikoPriceUsd6);
    }

    /// @notice Set operator allowed to settle queued pending redemptions.
    /// @param operator Operator address.
    function setPendingRedemptionOperator(address operator) external onlyOwner {
        if (operator == address(0)) revert RV_InvalidPendingRedemptionOperator();
        pendingRedemptionOperator = operator;
        emit PendingRedemptionOperatorUpdated(operator);
    }

    /// @notice Set wrapped native token that should be unwrapped and paid as native ETH on redeem.
    /// @param token Wrapped native token address. Zero disables auto-unwrap path.
    function setWrappedNativeToken(address token) external onlyOwner {
        wrappedNativeToken = token;
        emit WrappedNativeTokenUpdated(token);
    }

    /// @notice Add or update token whitelist config and oracle constraints.
    /// @param token Token address.
    /// @param allowed Whether token is enabled for deposit/redeem.
    /// @param oracle Chainlink feed address for token/USD.
    /// @param maxOracleAge Maximum accepted feed age in seconds.
    /// @param expectedFeedDescriptionHash Keccak hash of expected feed description.
    function setTokenConfig(
        address token,
        bool allowed,
        address oracle,
        uint32 maxOracleAge,
        bytes32 expectedFeedDescriptionHash
    ) external onlyOwner {
        if (token == address(0)) revert RV_UnsupportedToken();
        uint8 tokenDecimals = 0;
        if (allowed) {
            tokenDecimals = IERC20Metadata(token).decimals();
            _requireTokenDecimals(tokenDecimals);
            _validateOracleConfig(oracle, maxOracleAge, expectedFeedDescriptionHash);
        }
        tokenConfigs[token] = TokenConfig({
            allowed: allowed,
            oracle: oracle,
            maxOracleAge: maxOracleAge,
            expectedFeedDescriptionHash: expectedFeedDescriptionHash,
            tokenDecimals: allowed ? tokenDecimals : 0
        });
        emit TokenConfigUpdated(token, allowed, oracle, maxOracleAge, expectedFeedDescriptionHash);
    }

    /*//////////////////////////////////////////////////////////////
                             USER FUNCTIONS
    //////////////////////////////////////////////////////////////*/

    /// @notice Deposit whitelisted token and mint RIKO to receiver.
    /// @dev Security: uses pull-then-accounting flow with `nonReentrant`, strict oracle checks,
    ///      global/per-token caps, and forwards received funds to custody.
    /// @param token Input token address.
    /// @param amountIn Token amount requested from sender.
    /// @param minRikoOut Minimum accepted RIKO mint amount.
    /// @param receiver Receiver of minted RIKO.
    /// @return rikoOut Minted RIKO amount.
    function deposit(address token, uint256 amountIn, uint256 minRikoOut, address receiver)
        external
        nonReentrant
        whenNotPaused
        returns (uint256 rikoOut)
    {
        _requireValidReceiverAndPositiveAmount(receiver, amountIn);
        TokenConfig memory cfg = _loadAllowedTokenConfig(token);

        IERC20Metadata asset = IERC20Metadata(token);
        uint256 beforeBal = asset.balanceOf(address(this));
        asset.safeTransferFrom(msg.sender, address(this), amountIn);
        uint256 afterBal = asset.balanceOf(address(this));
        uint256 received = afterBal - beforeBal;
        if (received == 0) revert RV_InvalidAmount();

        (uint256 price, uint8 oracleDecimals) = _readOracle(cfg);
        uint256 usd6 = _tokenToUsd6WithOracle(cfg, received, price, oracleDecimals);
        if (usd6 == 0) revert RV_OraclePriceInvalid();
        rikoOut = _usd6ToRiko(usd6);
        if (rikoOut == 0) revert RV_InvalidAmount();
        if (rikoOut < minRikoOut) revert RV_SlippageExceeded();
        _checkGlobalCapAfterMint(rikoOut);

        _mint(receiver, rikoOut);
        if (!keepOnVaultByToken[token]) {
            address custody = _requireConfiguredCustody();
            asset.safeTransfer(custody, received);
        }
        _checkTokenCapAfterDeposit(token, cfg, asset, price, oracleDecimals);
        emit Deposited(receiver, token, received, rikoOut);
    }

    /// @notice Redeem RIKO to selected whitelisted token.
    /// @dev Security: enforces min-out check, and if liquidity is insufficient,
    ///      queues redemption instead of force-reverting state.
    /// @param token Output token address.
    /// @param rikoAmountIn Amount of RIKO to redeem.
    /// @param minTokenOut Minimum accepted token output amount.
    /// @param receiver Receiver of output tokens.
    /// @return tokenOut Output token amount, or zero if queued.
    function redeem(address token, uint256 rikoAmountIn, uint256 minTokenOut, address receiver)
        external
        nonReentrant
        whenNotPaused
        returns (uint256 tokenOut)
    {
        _requireValidReceiverAndPositiveAmount(receiver, rikoAmountIn);
        TokenConfig memory cfg = _loadAllowedTokenConfig(token);

        tokenOut = _usd6ToToken(cfg, _rikoToUsd6(rikoAmountIn));
        if (tokenOut < minTokenOut) revert RV_SlippageExceeded();

        // Auto-redeem path is strictly vault-only.
        // Custody funds are reserved for operator-driven pending settlement.
        if (_canSettleFromVault(token, tokenOut)) {
            _settleRedeem(msg.sender, token, receiver, rikoAmountIn, tokenOut, minTokenOut, false);
            return tokenOut;
        }

        _transfer(msg.sender, address(this), rikoAmountIn);
        _pendingRedemptionQueue[msg.sender][token].push(
            PendingRedemption({rikoLocked: rikoAmountIn, tokenOut: tokenOut, minTokenOut: minTokenOut, receiver: receiver})
        );
        emit RedeemQueued(msg.sender, token, receiver, rikoAmountIn, tokenOut, minTokenOut);
        return 0;
    }

    /// @notice Operator-only settlement of a queued redemption.
    /// @dev Security: restricted to configured operator to avoid arbitrary third-party
    ///      triggering of custody pull attempts.
    /// @param account Account that owns queued redemption.
    /// @param token Token address for queued redemption.
    /// @return completed True when settlement executed.
    /// @return tokenSent Actual token amount transferred to receiver.
    function processPendingRedemption(address account, address token)
        external
        nonReentrant
        whenNotPaused
        returns (bool completed, uint256 tokenSent)
    {
        if (msg.sender != pendingRedemptionOperator) revert RV_PendingRedemptionOperatorOnly();
        return _processPendingRedemption(account, token);
    }

    /// @notice Operator-only batch settlement for queued redemptions.
    /// @param account Account that owns queued redemptions.
    /// @param token Token address for queued redemptions.
    /// @param maxItems Max number of queue items to process in this call.
    /// @return processed Number of redemptions completed.
    /// @return totalTokenSent Total token amount transferred to receiver(s).
    function processPendingRedemptions(address account, address token, uint256 maxItems)
        external
        nonReentrant
        whenNotPaused
        returns (uint256 processed, uint256 totalTokenSent)
    {
        if (msg.sender != pendingRedemptionOperator) revert RV_PendingRedemptionOperatorOnly();
        _requirePositiveAmount(maxItems);
        for (uint256 i = 0; i < maxItems; ++i) {
            if (!_hasPendingRedemption(account, token)) break;
            (bool completed, uint256 tokenSent) = _processPendingRedemption(account, token);
            if (!completed) break;
            unchecked {
                ++processed;
            }
            totalTokenSent += tokenSent;
        }
        return (processed, totalTokenSent);
    }

    function _processPendingRedemption(address account, address token)
        internal
        returns (bool completed, uint256 tokenSent)
    {
        PendingRedemption memory p = _pendingRedemptionPeek(account, token);
        TokenConfig memory cfg = _loadAllowedTokenConfig(token);
        uint256 tokenOutCurrent = _usd6ToToken(cfg, _rikoToUsd6(p.rikoLocked));
        if (tokenOutCurrent < p.minTokenOut) revert RV_SlippageExceeded();
        // Do not overpay relative to the amount locked in queue at redeem time.
        uint256 tokenOutToSend = tokenOutCurrent > p.tokenOut ? p.tokenOut : tokenOutCurrent;
        // Pending settlement may use vault + custody pull.
        if (!_canSettleWithCustody(token, tokenOutToSend)) {
            return (false, 0);
        }
        tokenSent = _settlePendingRedeem(account, token, p, tokenOutToSend);
        return (true, tokenSent);
    }

    /// @notice Cancel caller's queued redemption and return locked RIKO.
    /// @param token Token key used for queued redemption.
    /// @return rikoReturned Amount of RIKO returned to caller.
    function cancelPendingRedemption(address token) external nonReentrant whenNotPaused returns (uint256 rikoReturned) {
        PendingRedemption memory p = _pendingRedemptionPeek(msg.sender, token);
        rikoReturned = p.rikoLocked;
        _pendingRedemptionPop(msg.sender, token);
        _transfer(address(this), msg.sender, rikoReturned);
        emit RedeemCancelled(msg.sender, token, rikoReturned);
    }

    /// @notice Returns the oldest pending redemption for (account, token).
    /// @dev Backward-compatible shape for UI integrations.
    function pendingRedemptions(address account, address token)
        external
        view
        returns (uint256 rikoLocked, uint256 tokenOut, uint256 minTokenOut, address receiver, bool exists)
    {
        uint256 head = _pendingRedemptionHead[account][token];
        PendingRedemption[] storage q = _pendingRedemptionQueue[account][token];
        if (head >= q.length) {
            return (0, 0, 0, address(0), false);
        }
        PendingRedemption storage p = q[head];
        return (p.rikoLocked, p.tokenOut, p.minTokenOut, p.receiver, true);
    }

    /// @notice Quote RIKO mint amount for token deposit input.
    /// @param token Input token address.
    /// @param amountIn Input token amount.
    /// @return rikoOut Expected RIKO output.
    function quoteDeposit(address token, uint256 amountIn) external view returns (uint256 rikoOut) {
        TokenConfig memory cfg = _loadAllowedTokenConfig(token);
        rikoOut = _tokenToUsd6(cfg, amountIn);
    }

    /// @notice Quote token output amount for RIKO redemption input.
    /// @param token Output token address.
    /// @param rikoIn RIKO redemption amount.
    /// @return tokenOut Expected token output.
    function quoteRedeem(address token, uint256 rikoIn) external view returns (uint256 tokenOut) {
        TokenConfig memory cfg = _loadAllowedTokenConfig(token);
        tokenOut = _usd6ToToken(cfg, _rikoToUsd6(rikoIn));
    }

    /*//////////////////////////////////////////////////////////////
                             INTERNAL LOGIC
    //////////////////////////////////////////////////////////////*/

    function _loadAllowedTokenConfig(address token) internal view returns (TokenConfig memory cfg) {
        cfg = tokenConfigs[token];
        if (!cfg.allowed) revert RV_UnsupportedToken();
    }

    function _requireValidReceiver(address receiver) internal pure {
        if (receiver == address(0)) revert RV_InvalidReceiver();
    }

    function _requirePositiveAmount(uint256 amount) internal pure {
        if (amount == 0) revert RV_InvalidAmount();
    }

    function _requireValidReceiverAndPositiveAmount(address receiver, uint256 amount) internal pure {
        _requireValidReceiver(receiver);
        _requirePositiveAmount(amount);
    }

    function _canSettleFromVault(address token, uint256 tokenOut) internal view returns (bool) {
        IERC20Metadata asset = IERC20Metadata(token);
        uint256 avail = asset.balanceOf(address(this));
        return avail >= tokenOut;
    }

    function _canSettleWithCustody(address token, uint256 tokenOut) internal view returns (bool) {
        IERC20Metadata asset = IERC20Metadata(token);
        uint256 avail = asset.balanceOf(address(this));
        if (avail >= tokenOut) return true;
        address custody = custodyAddress;
        if (custody == address(0)) return false;
        uint256 missing = tokenOut - avail;
        uint256 custodyBal = asset.balanceOf(custody);
        if (custodyBal < missing) return false;
        uint256 custodyAllowance = asset.allowance(custody, address(this));
        return custodyAllowance >= missing;
    }

    function _pullFromCustodyIfNeeded(address token, uint256 tokenOut) internal {
        IERC20Metadata asset = IERC20Metadata(token);
        uint256 avail = asset.balanceOf(address(this));
        if (avail >= tokenOut) return;
        address custody = _requireConfiguredCustody();
        uint256 missing = tokenOut - avail;
        asset.safeTransferFrom(custody, address(this), missing);
    }

    function _settleRedeem(
        address account,
        address token,
        address receiver,
        uint256 rikoAmountIn,
        uint256 tokenOut,
        uint256 minTokenOut,
        bool allowCustodyPull
    ) internal {
        IERC20Metadata asset = IERC20Metadata(token);
        if (allowCustodyPull) {
            _pullFromCustodyIfNeeded(token, tokenOut);
        }
        uint256 avail = asset.balanceOf(address(this));
        if (avail < tokenOut) revert RV_InsufficientLiquidity();
        _transferOutWithMinOutCheck(token, asset, receiver, tokenOut, minTokenOut);
        _burn(account, rikoAmountIn);
        emit Redeemed(receiver, token, rikoAmountIn, tokenOut);
        emit RedeemCompleted(account, token, receiver, rikoAmountIn, tokenOut);
    }

    function _settlePendingRedeem(address account, address token, PendingRedemption memory p, uint256 tokenOutCurrent)
        internal
        returns (uint256 tokenSent)
    {
        uint256 rikoLocked = p.rikoLocked;
        uint256 minTokenOut = p.minTokenOut;
        address receiver = p.receiver;
        _pendingRedemptionPop(account, token);
        _settleRedeem(address(this), token, receiver, rikoLocked, tokenOutCurrent, minTokenOut, true);
        tokenSent = tokenOutCurrent;
    }

    function _pendingRedemptionPeek(address account, address token) internal view returns (PendingRedemption memory p) {
        uint256 head = _pendingRedemptionHead[account][token];
        PendingRedemption[] storage q = _pendingRedemptionQueue[account][token];
        if (head >= q.length) revert RV_PendingRedemptionNotFound();
        p = q[head];
    }

    function _hasPendingRedemption(address account, address token) internal view returns (bool) {
        uint256 head = _pendingRedemptionHead[account][token];
        PendingRedemption[] storage q = _pendingRedemptionQueue[account][token];
        return head < q.length;
    }

    function _pendingRedemptionPop(address account, address token) internal {
        uint256 head = _pendingRedemptionHead[account][token];
        PendingRedemption[] storage q = _pendingRedemptionQueue[account][token];
        if (head >= q.length) revert RV_PendingRedemptionNotFound();
        unchecked {
            _pendingRedemptionHead[account][token] = head + 1;
        }
    }

    function _tokenToUsd6(TokenConfig memory cfg, uint256 tokenAmount) internal view returns (uint256) {
        (uint256 price, uint8 oracleDecimals) = _readOracle(cfg);
        return _tokenToUsd6WithOracle(cfg, tokenAmount, price, oracleDecimals);
    }

    function _tokenToUsd6WithOracle(TokenConfig memory cfg, uint256 tokenAmount, uint256 price, uint8 oracleDecimals)
        internal
        pure
        returns (uint256)
    {
        _requireTokenDecimals(cfg.tokenDecimals);
        // usd = tokenAmount * price / 10**tokenDecimals, normalize to 6 decimals.
        // tokenAmount(10^tokenDec) * price(10^oracleDec) -> 10^(tokenDec+oracleDec)
        uint256 rawUsd = (tokenAmount * price) / (10 ** cfg.tokenDecimals);
        return _rescaleDecimals(rawUsd, oracleDecimals, USD_DECIMALS);
    }

    function _usd6ToToken(TokenConfig memory cfg, uint256 usd6) internal view returns (uint256) {
        (uint256 price, uint8 oracleDecimals) = _readOracle(cfg);
        return _usd6ToTokenWithOracle(cfg, usd6, price, oracleDecimals);
    }

    function _usd6ToTokenWithOracle(TokenConfig memory cfg, uint256 usd6, uint256 price, uint8 oracleDecimals)
        internal
        pure
        returns (uint256)
    {
        _requireTokenDecimals(cfg.tokenDecimals);
        // tokenAmount = usd * 10^tokenDecimals / price
        // usd6 -> oracle decimals
        uint256 usdOracleScaled = _rescaleDecimals(usd6, USD_DECIMALS, oracleDecimals);
        return (usdOracleScaled * (10 ** cfg.tokenDecimals)) / price;
    }

    function _readOracle(TokenConfig memory cfg) internal view returns (uint256 price, uint8 oracleDecimals) {
        if (cfg.oracle == address(0)) revert RV_InvalidOracle();
        IChainlinkAggregatorV3 feed = IChainlinkAggregatorV3(cfg.oracle);
        oracleDecimals = _validateFeedIdentity(feed, cfg.expectedFeedDescriptionHash);
        (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound) =
            feed.latestRoundData();
        if (roundId == 0 || startedAt > updatedAt || answeredInRound < roundId) revert RV_OraclePriceInvalid();
        if (answer <= 0) revert RV_OraclePriceInvalid();
        if (updatedAt > block.timestamp) revert RV_OraclePriceInvalid();
        if (updatedAt == 0 || block.timestamp - updatedAt > cfg.maxOracleAge) revert RV_OraclePriceStale();
        // Safe because `answer > 0` is enforced above.
        price = uint256(answer);
    }

    function _requireTokenDecimals(uint8 tokenDecimals) internal pure {
        if (tokenDecimals < MIN_DECIMALS || tokenDecimals > MAX_DECIMALS) revert RV_TokenDecimalsInvalid();
    }

    function _requireOracleDecimals(uint8 oracleDecimals) internal pure {
        if (oracleDecimals < MIN_DECIMALS || oracleDecimals > MAX_DECIMALS) revert RV_OracleDecimalsInvalid();
    }

    function _rescaleDecimals(uint256 amount, uint8 fromDecimals, uint8 toDecimals) internal pure returns (uint256) {
        if (fromDecimals > toDecimals) {
            return amount / (10 ** (fromDecimals - toDecimals));
        }
        if (fromDecimals < toDecimals) {
            return amount * (10 ** (toDecimals - fromDecimals));
        }
        return amount;
    }

    function _checkGlobalCapAfterMint(uint256 rikoOut) internal view {
        uint256 capGlobal = globalSupplyCapUsd6;
        if (capGlobal == 0) return;
        uint256 totalUsd6After = _rikoToUsd6(totalSupply() + rikoOut);
        if (totalUsd6After > capGlobal) revert RV_GlobalCapExceeded();
    }

    function _checkTokenCapAfterDeposit(
        address token,
        TokenConfig memory cfg,
        IERC20Metadata asset,
        uint256 oraclePrice,
        uint8 oracleDecimals
    ) internal view {
        uint256 capToken = tokenTvlCapUsd6[token];
        if (capToken == 0) return;
        uint256 totalTokenExposure = asset.balanceOf(address(this));
        address custody = custodyAddress;
        if (custody != address(0)) {
            totalTokenExposure += asset.balanceOf(custody);
        }
        uint256 tvlTokenUsd6After = _tokenToUsd6WithOracle(cfg, totalTokenExposure, oraclePrice, oracleDecimals);
        if (tvlTokenUsd6After > capToken) revert RV_TokenCapExceeded();
    }

    function _safeTransferWithMinOutCheck(
        IERC20Metadata asset,
        address receiver,
        uint256 transferAmount,
        uint256 minTokenOut
    ) internal {
        uint256 receiverBefore = asset.balanceOf(receiver);
        asset.safeTransfer(receiver, transferAmount);
        uint256 receiverAfter = asset.balanceOf(receiver);
        if (receiverAfter - receiverBefore < minTokenOut) revert RV_SlippageExceeded();
    }

    function _transferOutWithMinOutCheck(
        address token,
        IERC20Metadata asset,
        address receiver,
        uint256 tokenOut,
        uint256 minTokenOut
    ) internal {
        if (_isWrappedNativeToken(token)) {
            if (tokenOut < minTokenOut) revert RV_SlippageExceeded();
            IWETHLike(token).withdraw(tokenOut);
            (bool ok,) = receiver.call{value: tokenOut}("");
            if (!ok) revert RV_NativeTransferFailed();
            return;
        }
        _safeTransferWithMinOutCheck(asset, receiver, tokenOut, minTokenOut);
    }

    function _isWrappedNativeToken(address token) internal view returns (bool) {
        address wrapped = wrappedNativeToken;
        return wrapped != address(0) && token == wrapped;
    }

    function _requireConfiguredCustody() internal view returns (address custody) {
        custody = custodyAddress;
        if (custody == address(0)) revert RV_InvalidCustodyAddress();
    }

    function _validateOracleConfig(address oracle, uint32 maxOracleAge, bytes32 expectedFeedDescriptionHash)
        internal
        view
    {
        if (oracle == address(0)) revert RV_InvalidOracle();
        if (maxOracleAge == 0) revert RV_OraclePriceInvalid();
        if (expectedFeedDescriptionHash == bytes32(0)) revert RV_OracleFeedMismatch();
        IChainlinkAggregatorV3 feed = IChainlinkAggregatorV3(oracle);
        _validateFeedIdentity(feed, expectedFeedDescriptionHash);
    }

    function _validateFeedIdentity(IChainlinkAggregatorV3 feed, bytes32 expectedFeedDescriptionHash)
        internal
        view
        returns (uint8 oracleDecimals)
    {
        oracleDecimals = feed.decimals();
        _requireOracleDecimals(oracleDecimals);
        if (expectedFeedDescriptionHash == bytes32(0)) return oracleDecimals;
        bytes32 gotHash = keccak256(bytes(feed.description()));
        if (gotHash != expectedFeedDescriptionHash) revert RV_OracleFeedMismatch();
    }

    function _usd6ToRiko(uint256 usd6) internal view returns (uint256) {
        uint256 price = _rikoPriceOrRevert();
        return (usd6 * RIKO_DECIMALS_SCALE) / price;
    }

    function _rikoToUsd6(uint256 rikoAmount) internal view returns (uint256) {
        uint256 price = _rikoPriceOrRevert();
        return (rikoAmount * price) / RIKO_DECIMALS_SCALE;
    }

    function _rikoPriceOrRevert() internal view returns (uint256 price) {
        price = rikoPriceUsd6;
        if (price == 0) revert RV_InvalidRikoPrice();
    }

    function _defaultWrappedNativeToken(uint256 chainId) internal pure returns (address) {
        if (chainId == 1) return 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2;
        if (chainId == 11155111) return 0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14;
        return address(0);
    }

    receive() external payable {}
}
