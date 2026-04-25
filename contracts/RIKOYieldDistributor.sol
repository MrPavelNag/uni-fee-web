// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {IERC20Metadata} from "@openzeppelin/contracts/token/ERC20/extensions/IERC20Metadata.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";
import {Ownable2Step} from "@openzeppelin/contracts/access/Ownable2Step.sol";
import {ReentrancyGuard} from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import {Pausable} from "@openzeppelin/contracts/utils/Pausable.sol";
import {IRIKOToken} from "./interfaces/IRIKOToken.sol";

contract RIKOYieldDistributor is Ownable2Step, ReentrancyGuard, Pausable {
    using SafeERC20 for IERC20Metadata;

    /*//////////////////////////////////////////////////////////////
                                CONSTANTS
    //////////////////////////////////////////////////////////////*/

    uint256 public constant BPS_DENOMINATOR = 10_000;

    /*//////////////////////////////////////////////////////////////
                                  ERRORS
    //////////////////////////////////////////////////////////////*/

    error RYD_InvalidAmount();
    error RYD_InvalidReceiver();
    error RYD_InvalidYieldPayerAddress();
    error RYD_InvalidYieldToken();
    error RYD_YieldNotReady();
    error RYD_YieldAccountMismatch();
    error RYD_InvalidRikoToken();
    error RYD_ClaimDisabled();
    error RYD_InvalidCycle();

    /*//////////////////////////////////////////////////////////////
                                  STORAGE
    //////////////////////////////////////////////////////////////*/

    IRIKOToken public immutable rikoToken;
    address public yieldPayerAddress;
    address public yieldTokenAddress;
    uint256 public monthlyYieldRateBps;
    uint256 public currentYieldCycle;
    mapping(uint256 => uint256) public yieldRateBpsByCycle;
    mapping(uint256 => mapping(address => bool)) public wasPaidInCycle;

    /*//////////////////////////////////////////////////////////////
                                   EVENTS
    //////////////////////////////////////////////////////////////*/

    event YieldPayerAddressUpdated(address indexed yieldPayerAddress);
    event YieldTokenAddressUpdated(address indexed yieldTokenAddress);
    event MonthlyYieldRateUpdated(uint256 monthlyYieldRateBps, uint256 yieldCycle);
    event YieldPaid(address indexed account, uint256 amount, uint256 yieldCycle, uint256 rateBps);
    event YieldBatchPaid(uint256 indexed yieldCycle, uint256 accountsProcessed, uint256 totalPayout, uint256 rateBps);

    /*//////////////////////////////////////////////////////////////
                                 LIFECYCLE
    //////////////////////////////////////////////////////////////*/

    /// @notice Create distributor for a specific RIKO token contract.
    /// @param admin Owner address.
    /// @param rikoTokenAddress RIKO token address used for balance snapshots.
    constructor(address admin, address rikoTokenAddress) Ownable(admin) {
        if (admin == address(0)) revert RYD_InvalidReceiver();
        _requireNonZeroRikoAddress(rikoTokenAddress);
        rikoToken = IRIKOToken(rikoTokenAddress);
        yieldPayerAddress = admin;
        emit YieldPayerAddressUpdated(admin);
    }

    /*//////////////////////////////////////////////////////////////
                             ADMIN FUNCTIONS
    //////////////////////////////////////////////////////////////*/

    /// @notice Pause claim operations.
    function pause() external onlyOwner {
        _pause();
    }

    /// @notice Unpause claim operations.
    function unpause() external onlyOwner {
        _unpause();
    }

    /// @notice Set wallet that provides yield token transfers on claim.
    /// @param newYieldPayerAddress Address that approves distributor for payouts.
    function setYieldPayerAddress(address newYieldPayerAddress) external onlyOwner {
        _requireNonZeroPayerAddress(newYieldPayerAddress);
        yieldPayerAddress = newYieldPayerAddress;
        emit YieldPayerAddressUpdated(newYieldPayerAddress);
    }

    /// @notice Set ERC20 token used for yield payouts.
    /// @param newYieldTokenAddress Payout token address.
    function setYieldTokenAddress(address newYieldTokenAddress) external onlyOwner {
        if (newYieldTokenAddress == address(0)) revert RYD_InvalidYieldToken();
        yieldTokenAddress = newYieldTokenAddress;
        emit YieldTokenAddressUpdated(newYieldTokenAddress);
    }

    /// @notice Set monthly rate in bps and open a new claim cycle.
    /// @param newMonthlyYieldRateBps Monthly percentage in basis points.
    function setMonthlyYieldRateBps(uint256 newMonthlyYieldRateBps) external onlyOwner {
        if (newMonthlyYieldRateBps > BPS_DENOMINATOR) revert RYD_InvalidAmount();
        monthlyYieldRateBps = newMonthlyYieldRateBps;
        currentYieldCycle += 1;
        yieldRateBpsByCycle[currentYieldCycle] = newMonthlyYieldRateBps;
        emit MonthlyYieldRateUpdated(newMonthlyYieldRateBps, currentYieldCycle);
    }

    /*//////////////////////////////////////////////////////////////
                             USER FUNCTIONS
    //////////////////////////////////////////////////////////////*/

    /// @notice Backward-compatible alias for `claimMonthlyYield`.
    /// @return payoutAmount Payout amount transferred to caller.
    function claimDailyYield() external nonReentrant whenNotPaused returns (uint256) {
        // Pull-claim is deprecated; payouts are now pushed by operator/owner.
        revert RYD_ClaimDisabled();
    }

    /// @notice Backward-compatible alias for `claimMonthlyYieldFor`.
    /// @param account Receiver account (must match `msg.sender`).
    /// @return payoutAmount Payout amount transferred to account.
    function claimDailyYieldFor(address account) external nonReentrant whenNotPaused returns (uint256) {
        account; // silence lint about unused argument in deprecated path
        revert RYD_ClaimDisabled();
    }

    /// @notice Claim current cycle payout based on caller's RIKO balance.
    /// @dev Security: cycle gate prevents double-claim in same cycle; payout amount is
    ///      based on current RIKO balance and paid via `safeTransferFrom`.
    /// @return payoutAmount Payout amount transferred to caller.
    function claimMonthlyYield() external nonReentrant whenNotPaused returns (uint256) {
        revert RYD_ClaimDisabled();
    }

    /// @notice Claim current cycle payout for account.
    /// @param account Receiver account (must match `msg.sender`).
    /// @return payoutAmount Payout amount transferred to account.
    function claimMonthlyYieldFor(address account) external nonReentrant whenNotPaused returns (uint256) {
        account; // silence lint about unused argument in deprecated path
        revert RYD_ClaimDisabled();
    }

    /// @notice Push payout to holders for a specific cycle in batch.
    /// @dev Caller provides holder list (on-chain holder enumeration is not feasible gas-wise).
    ///      Payout per holder = `rikoBalance * cycleRateBps / 10_000`.
    ///      Each holder can be paid once per cycle.
    /// @param cycle Yield cycle number to settle.
    /// @param accounts Holder addresses to pay.
    /// @return totalPayout Total amount transferred in this batch.
    function distributeYieldBatch(uint256 cycle, address[] calldata accounts)
        external
        onlyOwner
        nonReentrant
        whenNotPaused
        returns (uint256 totalPayout)
    {
        address payer = yieldPayerAddress;
        address yieldToken = yieldTokenAddress;
        if (payer == address(0)) revert RYD_InvalidYieldPayerAddress();
        if (yieldToken == address(0)) revert RYD_InvalidYieldToken();
        if (cycle == 0 || cycle > currentYieldCycle) revert RYD_InvalidCycle();

        uint256 rateBps = yieldRateBpsByCycle[cycle];
        IERC20Metadata payoutToken = IERC20Metadata(yieldToken);
        uint256 len = accounts.length;
        for (uint256 i = 0; i < len; ++i) {
            address account = accounts[i];
            if (account == address(0)) revert RYD_InvalidReceiver();
            if (wasPaidInCycle[cycle][account]) continue;

            wasPaidInCycle[cycle][account] = true;
            uint256 balance = rikoToken.balanceOf(account);
            uint256 payoutAmount = (balance * rateBps) / BPS_DENOMINATOR;
            if (payoutAmount > 0) {
                payoutToken.safeTransferFrom(payer, account, payoutAmount);
                totalPayout += payoutAmount;
            }
            emit YieldPaid(account, payoutAmount, cycle, rateBps);
        }
        emit YieldBatchPaid(cycle, len, totalPayout, rateBps);
    }

    function _requireNonZeroPayerAddress(address value) internal pure {
        if (value == address(0)) revert RYD_InvalidYieldPayerAddress();
    }

    function _requireNonZeroRikoAddress(address value) internal pure {
        if (value == address(0)) revert RYD_InvalidRikoToken();
    }
}
