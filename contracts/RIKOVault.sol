// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {IERC20Metadata} from "@openzeppelin/contracts/token/ERC20/extensions/IERC20Metadata.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";
import {Ownable2Step} from "@openzeppelin/contracts/access/Ownable2Step.sol";
import {ReentrancyGuard} from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import {Pausable} from "@openzeppelin/contracts/utils/Pausable.sol";

interface IChainlinkAggregatorV3 {
    function latestRoundData()
        external
        view
        returns (uint80, int256, uint256, uint256, uint80);

    function decimals() external view returns (uint8);
    function description() external view returns (string memory);
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

    uint8 private constant MIN_DECIMALS = 1;
    uint8 private constant MAX_DECIMALS = 18;
    uint8 private constant USD_DECIMALS = 6;

    error UnsupportedToken();
    error InvalidAmount();
    error InvalidReceiver();
    error InvalidOracle();
    error OracleFeedMismatch();
    error OraclePriceInvalid();
    error OraclePriceStale();
    error OracleDecimalsInvalid();
    error TokenDecimalsInvalid();
    error SlippageExceeded();
    error InsufficientLiquidity();
    error GlobalCapExceeded();
    error TokenCapExceeded();
    error InvalidCustodyAddress();
    error InvalidYieldPayerAddress();
    error InvalidYieldToken();
    error YieldNotReady();
    error PendingRedemptionExists();
    error PendingRedemptionNotFound();

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
        bool exists;
    }

    // 1 RIKO = 1 USD with 6 decimals (USDT-style UX).
    uint256 public constant RIKO_DECIMALS_SCALE = 1e6;
    uint256 public constant BPS_DENOMINATOR = 10_000;

    mapping(address => TokenConfig) public tokenConfigs;
    // 0 means "no cap".
    uint256 public globalSupplyCapUsd6;
    mapping(address => uint256) public tokenTvlCapUsd6;
    address public custodyAddress;
    address public yieldPayerAddress;
    address public yieldTokenAddress;
    uint256 public dailyYieldRateBps;
    mapping(address => uint256) public lastYieldDay;
    mapping(address => mapping(address => PendingRedemption)) public pendingRedemptions;

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
    event CustodyAddressUpdated(address indexed custodyAddress);
    event YieldPayerAddressUpdated(address indexed yieldPayerAddress);
    event YieldTokenAddressUpdated(address indexed yieldTokenAddress);
    event DailyYieldRateUpdated(uint256 dailyYieldRateBps);
    event YieldPaid(address indexed account, uint256 amount, uint256 daysElapsed, uint256 rateBps);
    event RedeemQueued(
        address indexed account,
        address indexed token,
        address indexed receiver,
        uint256 rikoLocked,
        uint256 tokenOut,
        uint256 minTokenOut
    );
    event RedeemCompleted(
        address indexed account,
        address indexed token,
        address indexed receiver,
        uint256 rikoBurned,
        uint256 tokenOut
    );

    constructor(address admin) ERC20("RIKO", "RIKO") Ownable(admin) {
        if (admin == address(0)) revert InvalidReceiver();
        custodyAddress = admin;
        yieldPayerAddress = admin;
        emit CustodyAddressUpdated(admin);
        emit YieldPayerAddressUpdated(admin);
    }

    function decimals() public pure override returns (uint8) {
        return 6;
    }

    function pause() external onlyOwner {
        _pause();
    }

    function unpause() external onlyOwner {
        _unpause();
    }

    function setGlobalSupplyCapUsd6(uint256 capUsd6) external onlyOwner {
        globalSupplyCapUsd6 = capUsd6;
        emit GlobalSupplyCapUpdated(capUsd6);
    }

    function setTokenTvlCapUsd6(address token, uint256 capUsd6) external onlyOwner {
        if (token == address(0)) revert UnsupportedToken();
        tokenTvlCapUsd6[token] = capUsd6;
        emit TokenTvlCapUpdated(token, capUsd6);
    }

    function setCustodyAddress(address newCustodyAddress) external onlyOwner {
        if (newCustodyAddress == address(0)) revert InvalidCustodyAddress();
        custodyAddress = newCustodyAddress;
        emit CustodyAddressUpdated(newCustodyAddress);
    }

    function setYieldPayerAddress(address newYieldPayerAddress) external onlyOwner {
        if (newYieldPayerAddress == address(0)) revert InvalidYieldPayerAddress();
        yieldPayerAddress = newYieldPayerAddress;
        emit YieldPayerAddressUpdated(newYieldPayerAddress);
    }

    function setYieldTokenAddress(address newYieldTokenAddress) external onlyOwner {
        if (newYieldTokenAddress == address(0)) revert InvalidYieldToken();
        yieldTokenAddress = newYieldTokenAddress;
        emit YieldTokenAddressUpdated(newYieldTokenAddress);
    }

    function setDailyYieldRateBps(uint256 newDailyYieldRateBps) external onlyOwner {
        dailyYieldRateBps = newDailyYieldRateBps;
        emit DailyYieldRateUpdated(newDailyYieldRateBps);
    }

    function setTokenConfig(
        address token,
        bool allowed,
        address oracle,
        uint32 maxOracleAge,
        bytes32 expectedFeedDescriptionHash
    ) external onlyOwner {
        if (token == address(0)) revert UnsupportedToken();
        uint8 tokenDecimals = 0;
        if (allowed) {
            tokenDecimals = IERC20Metadata(token).decimals();
            _requireTokenDecimals(tokenDecimals);
            if (oracle == address(0)) revert InvalidOracle();
            if (maxOracleAge == 0) revert OraclePriceInvalid();
            if (expectedFeedDescriptionHash == bytes32(0)) revert OracleFeedMismatch();
            IChainlinkAggregatorV3 feed = IChainlinkAggregatorV3(oracle);
            uint8 feedDecimals = feed.decimals();
            _requireOracleDecimals(feedDecimals);
            bytes32 gotHash = keccak256(bytes(feed.description()));
            if (gotHash != expectedFeedDescriptionHash) revert OracleFeedMismatch();
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

    function deposit(address token, uint256 amountIn, uint256 minRikoOut, address receiver)
        external
        nonReentrant
        whenNotPaused
        returns (uint256 rikoOut)
    {
        if (receiver == address(0)) revert InvalidReceiver();
        if (amountIn == 0) revert InvalidAmount();
        TokenConfig memory cfg = tokenConfigs[token];
        if (!cfg.allowed) revert UnsupportedToken();

        IERC20Metadata asset = IERC20Metadata(token);
        uint256 beforeBal = asset.balanceOf(address(this));
        asset.safeTransferFrom(msg.sender, address(this), amountIn);
        uint256 afterBal = asset.balanceOf(address(this));
        uint256 received = afterBal - beforeBal;
        if (received == 0) revert InvalidAmount();

        (uint256 price, uint8 oracleDecimals) = _readOracle(cfg);
        uint256 usd6 = _tokenToUsd6WithOracle(cfg, received, price, oracleDecimals);
        if (usd6 == 0) revert OraclePriceInvalid();
        rikoOut = usd6;
        if (rikoOut < minRikoOut) revert SlippageExceeded();
        uint256 capGlobal = globalSupplyCapUsd6;
        if (capGlobal > 0 && totalSupply() + rikoOut > capGlobal) revert GlobalCapExceeded();
        uint256 capToken = tokenTvlCapUsd6[token];
        if (capToken > 0) {
            uint256 tvlTokenUsd6After = _tokenToUsd6WithOracle(cfg, afterBal, price, oracleDecimals);
            if (tvlTokenUsd6After > capToken) revert TokenCapExceeded();
        }

        _mint(receiver, rikoOut);
        _initializeYieldDayIfNeeded(receiver);
        address custody = custodyAddress;
        if (custody == address(0)) revert InvalidCustodyAddress();
        asset.safeTransfer(custody, received);
        emit Deposited(receiver, token, received, rikoOut);
    }

    function redeem(address token, uint256 rikoAmountIn, uint256 minTokenOut, address receiver)
        external
        nonReentrant
        whenNotPaused
        returns (uint256 tokenOut)
    {
        if (receiver == address(0)) revert InvalidReceiver();
        if (rikoAmountIn == 0) revert InvalidAmount();
        TokenConfig memory cfg = tokenConfigs[token];
        if (!cfg.allowed) revert UnsupportedToken();

        tokenOut = _usd6ToToken(cfg, rikoAmountIn);
        if (tokenOut < minTokenOut) revert SlippageExceeded();

        if (_canSettleNow(token, tokenOut)) {
            _settleRedeem(msg.sender, token, receiver, rikoAmountIn, tokenOut, minTokenOut);
            return tokenOut;
        }

        PendingRedemption storage p = pendingRedemptions[msg.sender][token];
        if (p.exists) revert PendingRedemptionExists();
        _transfer(msg.sender, address(this), rikoAmountIn);
        p.rikoLocked = rikoAmountIn;
        p.tokenOut = tokenOut;
        p.minTokenOut = minTokenOut;
        p.receiver = receiver;
        p.exists = true;
        emit RedeemQueued(msg.sender, token, receiver, rikoAmountIn, tokenOut, minTokenOut);
        return 0;
    }

    function processPendingRedemption(address account, address token)
        external
        nonReentrant
        whenNotPaused
        returns (bool completed, uint256 tokenSent)
    {
        PendingRedemption storage p = pendingRedemptions[account][token];
        if (!p.exists) revert PendingRedemptionNotFound();
        if (!_canSettleNow(token, p.tokenOut)) {
            return (false, 0);
        }
        tokenSent = _settlePendingRedeem(account, token, p);
        return (true, tokenSent);
    }

    function quoteDeposit(address token, uint256 amountIn) external view returns (uint256 rikoOut) {
        TokenConfig memory cfg = tokenConfigs[token];
        if (!cfg.allowed) revert UnsupportedToken();
        rikoOut = _tokenToUsd6(cfg, amountIn);
    }

    function quoteRedeem(address token, uint256 rikoIn) external view returns (uint256 tokenOut) {
        TokenConfig memory cfg = tokenConfigs[token];
        if (!cfg.allowed) revert UnsupportedToken();
        tokenOut = _usd6ToToken(cfg, rikoIn);
    }

    function claimDailyYield() external nonReentrant whenNotPaused returns (uint256 payoutAmount) {
        return _claimDailyYieldFor(msg.sender);
    }

    function claimDailyYieldFor(address account) external nonReentrant whenNotPaused returns (uint256 payoutAmount) {
        return _claimDailyYieldFor(account);
    }

    function _claimDailyYieldFor(address account) internal returns (uint256 payoutAmount) {
        address payer = yieldPayerAddress;
        address yieldToken = yieldTokenAddress;
        if (payer == address(0)) revert InvalidYieldPayerAddress();
        if (yieldToken == address(0)) revert InvalidYieldToken();

        uint256 balance = balanceOf(account);
        if (balance == 0) revert InvalidAmount();

        uint256 currentDay = (block.timestamp / 1 days) + 1;
        uint256 lastDay = lastYieldDay[account];
        if (lastDay == 0) {
            lastYieldDay[account] = currentDay;
            revert YieldNotReady();
        }
        uint256 daysElapsed = currentDay - lastDay;
        if (daysElapsed == 0) revert YieldNotReady();

        uint256 rateBps = dailyYieldRateBps;
        if (rateBps == 0) {
            lastYieldDay[account] = currentDay;
            emit YieldPaid(account, 0, daysElapsed, 0);
            return 0;
        }

        payoutAmount = (balance * rateBps * daysElapsed) / BPS_DENOMINATOR;
        lastYieldDay[account] = currentDay;
        IERC20Metadata(yieldToken).safeTransferFrom(payer, account, payoutAmount);
        emit YieldPaid(account, payoutAmount, daysElapsed, rateBps);
    }

    function _canSettleNow(address token, uint256 tokenOut) internal view returns (bool) {
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
        address custody = custodyAddress;
        if (custody == address(0)) revert InvalidCustodyAddress();
        uint256 missing = tokenOut - avail;
        asset.safeTransferFrom(custody, address(this), missing);
    }

    function _settleRedeem(
        address account,
        address token,
        address receiver,
        uint256 rikoAmountIn,
        uint256 tokenOut,
        uint256 minTokenOut
    ) internal {
        IERC20Metadata asset = IERC20Metadata(token);
        _pullFromCustodyIfNeeded(token, tokenOut);
        uint256 avail = asset.balanceOf(address(this));
        if (avail < tokenOut) revert InsufficientLiquidity();
        uint256 receiverBefore = asset.balanceOf(receiver);
        asset.safeTransfer(receiver, tokenOut);
        uint256 receiverAfter = asset.balanceOf(receiver);
        if (receiverAfter - receiverBefore < minTokenOut) revert SlippageExceeded();
        _burn(account, rikoAmountIn);
        emit Redeemed(receiver, token, rikoAmountIn, tokenOut);
        emit RedeemCompleted(account, token, receiver, rikoAmountIn, tokenOut);
    }

    function _settlePendingRedeem(address account, address token, PendingRedemption storage p)
        internal
        returns (uint256 tokenSent)
    {
        uint256 rikoLocked = p.rikoLocked;
        uint256 tokenOut = p.tokenOut;
        uint256 minTokenOut = p.minTokenOut;
        address receiver = p.receiver;
        delete pendingRedemptions[account][token];
        _settleRedeem(address(this), token, receiver, rikoLocked, tokenOut, minTokenOut);
        tokenSent = tokenOut;
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
        return _scaleOracleAmountToUsd6(rawUsd, oracleDecimals);
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
        uint256 usdOracleScaled = _scaleUsd6ToOracleAmount(usd6, oracleDecimals);
        return (usdOracleScaled * (10 ** cfg.tokenDecimals)) / price;
    }

    function _readOracle(TokenConfig memory cfg) internal view returns (uint256 price, uint8 oracleDecimals) {
        if (cfg.oracle == address(0)) revert InvalidOracle();
        IChainlinkAggregatorV3 feed = IChainlinkAggregatorV3(cfg.oracle);
        oracleDecimals = feed.decimals();
        _requireOracleDecimals(oracleDecimals);
        bytes32 gotHash = keccak256(bytes(feed.description()));
        if (cfg.expectedFeedDescriptionHash != bytes32(0) && gotHash != cfg.expectedFeedDescriptionHash) {
            revert OracleFeedMismatch();
        }
        (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound) = feed
            .latestRoundData();
        if (roundId == 0 || startedAt > updatedAt || answeredInRound < roundId) revert OraclePriceInvalid();
        if (answer <= 0) revert OraclePriceInvalid();
        if (updatedAt == 0 || block.timestamp - updatedAt > cfg.maxOracleAge) revert OraclePriceStale();
        // Safe because `answer > 0` is enforced above.
        price = uint256(answer);
    }

    function _requireTokenDecimals(uint8 tokenDecimals) internal pure {
        if (tokenDecimals < MIN_DECIMALS || tokenDecimals > MAX_DECIMALS) revert TokenDecimalsInvalid();
    }

    function _requireOracleDecimals(uint8 oracleDecimals) internal pure {
        if (oracleDecimals < MIN_DECIMALS || oracleDecimals > MAX_DECIMALS) revert OracleDecimalsInvalid();
    }

    function _scaleOracleAmountToUsd6(uint256 amount, uint8 oracleDecimals) internal pure returns (uint256) {
        if (oracleDecimals > USD_DECIMALS) {
            return amount / (10 ** (oracleDecimals - USD_DECIMALS));
        }
        if (oracleDecimals < USD_DECIMALS) {
            return amount * (10 ** (USD_DECIMALS - oracleDecimals));
        }
        return amount;
    }

    function _scaleUsd6ToOracleAmount(uint256 usd6Amount, uint8 oracleDecimals) internal pure returns (uint256) {
        if (oracleDecimals > USD_DECIMALS) {
            return usd6Amount * (10 ** (oracleDecimals - USD_DECIMALS));
        }
        if (oracleDecimals < USD_DECIMALS) {
            return usd6Amount / (10 ** (USD_DECIMALS - oracleDecimals));
        }
        return usd6Amount;
    }

    function _initializeYieldDayIfNeeded(address account) internal {
        if (account == address(0)) return;
        if (lastYieldDay[account] == 0) {
            lastYieldDay[account] = (block.timestamp / 1 days) + 1;
        }
    }
}
