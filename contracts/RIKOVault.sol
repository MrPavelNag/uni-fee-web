// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {IERC20Metadata} from "@openzeppelin/contracts/token/ERC20/extensions/IERC20Metadata.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
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

    struct TokenConfig {
        bool allowed;
        address oracle;
        uint32 maxOracleAge;
        bytes32 expectedFeedDescriptionHash;
    }

    // 1 RIKO = 1 USD with 6 decimals (USDT-style UX).
    uint256 public constant RIKO_DECIMALS_SCALE = 1e6;

    mapping(address => TokenConfig) public tokenConfigs;
    // 0 means "no cap".
    uint256 public globalSupplyCapUsd6;
    mapping(address => uint256) public tokenTvlCapUsd6;

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

    constructor(address admin) ERC20("RIKO", "RIKO") Ownable(admin) {}

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

    function setTokenConfig(
        address token,
        bool allowed,
        address oracle,
        uint32 maxOracleAge,
        bytes32 expectedFeedDescriptionHash
    ) external onlyOwner {
        if (token == address(0)) revert UnsupportedToken();
        if (allowed) {
            uint8 tokenDecimals = IERC20Metadata(token).decimals();
            if (tokenDecimals == 0 || tokenDecimals > 18) revert TokenDecimalsInvalid();
            if (oracle == address(0)) revert InvalidOracle();
            if (maxOracleAge == 0) revert OraclePriceInvalid();
            if (expectedFeedDescriptionHash == bytes32(0)) revert OracleFeedMismatch();
            IChainlinkAggregatorV3 feed = IChainlinkAggregatorV3(oracle);
            uint8 feedDecimals = feed.decimals();
            if (feedDecimals == 0 || feedDecimals > 18) revert OracleDecimalsInvalid();
            bytes32 gotHash = keccak256(bytes(feed.description()));
            if (gotHash != expectedFeedDescriptionHash) revert OracleFeedMismatch();
        }
        tokenConfigs[token] = TokenConfig({
            allowed: allowed,
            oracle: oracle,
            maxOracleAge: maxOracleAge,
            expectedFeedDescriptionHash: expectedFeedDescriptionHash
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
        uint256 usd6 = _tokenToUsd6WithOracle(token, received, price, oracleDecimals);
        if (usd6 == 0) revert OraclePriceInvalid();
        rikoOut = usd6;
        if (rikoOut < minRikoOut) revert SlippageExceeded();
        uint256 capGlobal = globalSupplyCapUsd6;
        if (capGlobal > 0 && totalSupply() + rikoOut > capGlobal) revert GlobalCapExceeded();
        uint256 capToken = tokenTvlCapUsd6[token];
        if (capToken > 0) {
            uint256 tvlTokenUsd6After = _tokenToUsd6WithOracle(token, afterBal, price, oracleDecimals);
            if (tvlTokenUsd6After > capToken) revert TokenCapExceeded();
        }

        _mint(receiver, rikoOut);
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

        _burn(msg.sender, rikoAmountIn);

        tokenOut = _usd6ToToken(token, rikoAmountIn, cfg);
        if (tokenOut < minTokenOut) revert SlippageExceeded();

        IERC20Metadata asset = IERC20Metadata(token);
        uint256 avail = asset.balanceOf(address(this));
        if (avail < tokenOut) revert InsufficientLiquidity();
        asset.safeTransfer(receiver, tokenOut);

        emit Redeemed(receiver, token, rikoAmountIn, tokenOut);
    }

    function quoteDeposit(address token, uint256 amountIn) external view returns (uint256 rikoOut) {
        TokenConfig memory cfg = tokenConfigs[token];
        if (!cfg.allowed) revert UnsupportedToken();
        rikoOut = _tokenToUsd6(token, amountIn, cfg);
    }

    function quoteRedeem(address token, uint256 rikoIn) external view returns (uint256 tokenOut) {
        TokenConfig memory cfg = tokenConfigs[token];
        if (!cfg.allowed) revert UnsupportedToken();
        tokenOut = _usd6ToToken(token, rikoIn, cfg);
    }

    function _tokenToUsd6(address token, uint256 tokenAmount, TokenConfig memory cfg) internal view returns (uint256) {
        (uint256 price, uint8 oracleDecimals) = _readOracle(cfg);
        return _tokenToUsd6WithOracle(token, tokenAmount, price, oracleDecimals);
    }

    function _tokenToUsd6WithOracle(address token, uint256 tokenAmount, uint256 price, uint8 oracleDecimals)
        internal
        view
        returns (uint256)
    {
        uint8 tokenDecimals = IERC20Metadata(token).decimals();
        if (tokenDecimals == 0 || tokenDecimals > 18) revert TokenDecimalsInvalid();
        // usd = tokenAmount * price / 10**tokenDecimals, normalize to 6 decimals.
        // tokenAmount(10^tokenDec) * price(10^oracleDec) -> 10^(tokenDec+oracleDec)
        uint256 rawUsd = (tokenAmount * price) / (10 ** tokenDecimals);
        if (oracleDecimals > 6) {
            return rawUsd / (10 ** (oracleDecimals - 6));
        }
        if (oracleDecimals < 6) {
            return rawUsd * (10 ** (6 - oracleDecimals));
        }
        return rawUsd;
    }

    function _usd6ToToken(address token, uint256 usd6, TokenConfig memory cfg) internal view returns (uint256) {
        (uint256 price, uint8 oracleDecimals) = _readOracle(cfg);
        return _usd6ToTokenWithOracle(token, usd6, price, oracleDecimals);
    }

    function _usd6ToTokenWithOracle(address token, uint256 usd6, uint256 price, uint8 oracleDecimals)
        internal
        view
        returns (uint256)
    {
        uint8 tokenDecimals = IERC20Metadata(token).decimals();
        if (tokenDecimals == 0 || tokenDecimals > 18) revert TokenDecimalsInvalid();
        // tokenAmount = usd * 10^tokenDecimals / price
        // usd6 -> oracle decimals
        uint256 usdOracleScaled;
        if (oracleDecimals > 6) {
            usdOracleScaled = usd6 * (10 ** (oracleDecimals - 6));
        } else if (oracleDecimals < 6) {
            usdOracleScaled = usd6 / (10 ** (6 - oracleDecimals));
        } else {
            usdOracleScaled = usd6;
        }
        return (usdOracleScaled * (10 ** tokenDecimals)) / price;
    }

    function _readOracle(TokenConfig memory cfg) internal view returns (uint256 price, uint8 oracleDecimals) {
        if (cfg.oracle == address(0)) revert InvalidOracle();
        IChainlinkAggregatorV3 feed = IChainlinkAggregatorV3(cfg.oracle);
        oracleDecimals = feed.decimals();
        if (oracleDecimals == 0 || oracleDecimals > 18) revert OracleDecimalsInvalid();
        bytes32 gotHash = keccak256(bytes(feed.description()));
        if (cfg.expectedFeedDescriptionHash != bytes32(0) && gotHash != cfg.expectedFeedDescriptionHash) {
            revert OracleFeedMismatch();
        }
        (, int256 answer,, uint256 updatedAt,) = feed.latestRoundData();
        if (answer <= 0) revert OraclePriceInvalid();
        if (updatedAt == 0 || block.timestamp - updatedAt > cfg.maxOracleAge) revert OraclePriceStale();
        price = uint256(answer);
    }
}
