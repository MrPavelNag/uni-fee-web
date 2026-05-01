// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "forge-std/Test.sol";
import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";
import {Pausable} from "@openzeppelin/contracts/utils/Pausable.sol";
import {RIKOVault} from "../contracts/RIKOVault.sol";
import {MockERC20, MockMutableDecimalsERC20, MockFeeOnTransferERC20, MockAggregatorV3} from "./mocks/RIKOMocks.sol";

contract RIKOVaultNegativeTest is Test {
    RIKOVault internal vault;
    MockERC20 internal usdc;
    MockAggregatorV3 internal usdcUsdFeed;
    address internal alice = address(0xA11CE);

    function setUp() public {
        vault = new RIKOVault(address(this));
        usdc = new MockERC20("USD Coin", "USDC", 6);
        usdcUsdFeed = new MockAggregatorV3(8, "USDC / USD", 1e8);
        bytes32 feedHash = keccak256(bytes("USDC / USD"));
        vault.setTokenConfig(address(usdc), true, address(usdcUsdFeed), 1 days, feedHash);
        vault.setPendingRedemptionOperator(address(this));
        usdc.approve(address(vault), type(uint256).max);
        usdc.mint(alice, 1_000_000e6);
    }

    function testSetTokenConfigRevertsOnBadOracleDecimals() public {
        MockAggregatorV3 badFeedZero = new MockAggregatorV3(0, "USDC / USD", 1e8);
        bytes32 feedHash = keccak256(bytes("USDC / USD"));
        vm.expectRevert(RIKOVault.RV_OracleDecimalsInvalid.selector);
        vault.setTokenConfig(address(usdc), true, address(badFeedZero), 1 days, feedHash);

        MockAggregatorV3 badFeedTooHigh = new MockAggregatorV3(19, "USDC / USD", 1e8);
        vm.expectRevert(RIKOVault.RV_OracleDecimalsInvalid.selector);
        vault.setTokenConfig(address(usdc), true, address(badFeedTooHigh), 1 days, feedHash);
    }

    function testSetTokenConfigRevertsOnBadTokenDecimals() public {
        MockERC20 badTokenZero = new MockERC20("BAD", "BAD0", 0);
        bytes32 feedHash = keccak256(bytes("USDC / USD"));
        vm.expectRevert(RIKOVault.RV_TokenDecimalsInvalid.selector);
        vault.setTokenConfig(address(badTokenZero), true, address(usdcUsdFeed), 1 days, feedHash);

        MockERC20 badTokenTooHigh = new MockERC20("BAD", "BAD19", 19);
        vm.expectRevert(RIKOVault.RV_TokenDecimalsInvalid.selector);
        vault.setTokenConfig(address(badTokenTooHigh), true, address(usdcUsdFeed), 1 days, feedHash);
    }

    function testStaleBoundaryPassesAtMaxAgeAndRevertsAfter() public {
        bytes32 feedHash = keccak256(bytes("USDC / USD"));
        uint32 maxAge = 1 days;
        vault.setTokenConfig(address(usdc), true, address(usdcUsdFeed), maxAge, feedHash);
        vm.warp(uint256(maxAge) + 100);

        vm.startPrank(alice);
        usdc.approve(address(vault), 100e6);
        vm.stopPrank();

        // Exactly at boundary: block.timestamp - updatedAt == maxAge => allowed.
        usdcUsdFeed.setRoundData(1e8, block.timestamp - maxAge);
        vm.prank(alice);
        uint256 minted = vault.deposit(address(usdc), 10e6, 0, alice);
        assertEq(minted, 10e6, "boundary stale check should pass");

        // One second older than maxAge => stale revert.
        usdcUsdFeed.setRoundData(1e8, block.timestamp - maxAge - 1);
        vm.prank(alice);
        vm.expectRevert(RIKOVault.RV_OraclePriceStale.selector);
        vault.deposit(address(usdc), 10e6, 0, alice);
    }

    function testPauseBlocksDepositAndRedeem() public {
        vm.startPrank(alice);
        usdc.approve(address(vault), 100e6);
        vault.deposit(address(usdc), 20e6, 0, alice);
        vm.stopPrank();

        vault.pause();

        vm.startPrank(alice);
        vm.expectRevert(Pausable.EnforcedPause.selector);
        vault.deposit(address(usdc), 1e6, 0, alice);

        vm.expectRevert(Pausable.EnforcedPause.selector);
        vault.redeem(address(usdc), 1e6, 0, alice);
        vm.stopPrank();
    }

    function testPauseBlocksPendingProcessAndCancel() public {
        vm.startPrank(alice);
        usdc.approve(address(vault), 100e6);
        vault.deposit(address(usdc), 50e6, 0, alice);
        vm.stopPrank();

        usdc.transfer(address(0xBEEF), 45e6);
        vm.prank(alice);
        vault.redeem(address(usdc), 50e6, 0, alice);

        vault.pause();

        vm.expectRevert(Pausable.EnforcedPause.selector);
        vault.processPendingRedemption(alice, address(usdc));

        vm.prank(alice);
        vm.expectRevert(Pausable.EnforcedPause.selector);
        vault.cancelPendingRedemption(address(usdc));
    }

    function testRedeemQueuesOnInsufficientLiquidity() public {
        vm.startPrank(alice);
        usdc.approve(address(vault), 100e6);
        vault.deposit(address(usdc), 50e6, 0, alice);
        vm.stopPrank();

        // Simulate custody wallet becoming underfunded.
        usdc.transfer(address(0xBEEF), 45e6);

        vm.prank(alice);
        uint256 out = vault.redeem(address(usdc), 50e6, 0, alice);
        assertEq(out, 0, "redeem should be queued, not reverted");
        (uint256 rikoLocked, uint256 tokenOut,, address receiver, bool exists) =
            vault.pendingRedemptions(alice, address(usdc));
        assertTrue(exists, "pending redeem must exist");
        assertEq(rikoLocked, 50e6, "locked riko");
        assertEq(tokenOut, 50e6, "token out");
        assertEq(receiver, alice, "receiver");
    }

    function testConfigDisableAndReEnable() public {
        bytes32 feedHash = keccak256(bytes("USDC / USD"));
        vault.setTokenConfig(address(usdc), false, address(0), 0, bytes32(0));

        vm.startPrank(alice);
        usdc.approve(address(vault), 50e6);
        vm.expectRevert(RIKOVault.RV_UnsupportedToken.selector);
        vault.deposit(address(usdc), 10e6, 0, alice);
        vm.stopPrank();

        vault.setTokenConfig(address(usdc), true, address(usdcUsdFeed), 1 days, feedHash);

        vm.prank(alice);
        uint256 minted = vault.deposit(address(usdc), 10e6, 0, alice);
        assertEq(minted, 10e6, "must work after re-enable");
    }

    function testRedeemRevertsWhenFeeOnTransferBreaksMinOut() public {
        MockFeeOnTransferERC20 feeToken = new MockFeeOnTransferERC20("Fee USD", "fUSD", 6);
        MockAggregatorV3 feeFeed = new MockAggregatorV3(8, "FUSD / USD", 1e8);
        vault.setCustodyAddress(address(vault));
        vault.setTokenConfig(address(feeToken), true, address(feeFeed), 1 days, keccak256(bytes("FUSD / USD")));

        feeToken.mint(alice, 100e6);
        vm.startPrank(alice);
        feeToken.approve(address(vault), 100e6);
        vault.deposit(address(feeToken), 100e6, 0, alice);
        uint256 expectedOut = vault.quoteRedeem(address(feeToken), 50e6);
        vm.expectRevert(RIKOVault.RV_SlippageExceeded.selector);
        vault.redeem(address(feeToken), 50e6, expectedOut, alice);
        vm.stopPrank();
    }

    function testDepositUsesPinnedDecimalsEvenIfTokenMetadataChanges() public {
        MockMutableDecimalsERC20 mutableToken = new MockMutableDecimalsERC20("Mutable USD", "mUSD", 6);
        MockAggregatorV3 mutableFeed = new MockAggregatorV3(8, "MUSD / USD", 1e8);
        vault.setTokenConfig(address(mutableToken), true, address(mutableFeed), 1 days, keccak256(bytes("MUSD / USD")));
        mutableToken.mint(alice, 100e6);
        mutableToken.setDecimals(18);

        vm.startPrank(alice);
        mutableToken.approve(address(vault), 100e6);
        uint256 minted = vault.deposit(address(mutableToken), 10e6, 0, alice);
        vm.stopPrank();
        assertEq(minted, 10e6, "must use pinned decimals from listing time");
    }

    function testDepositRevertsOnInvalidOracleRoundMetadata() public {
        uint256 ts = block.timestamp + 100;
        usdcUsdFeed.setRoundMeta(2, 1, ts - 10, ts - 5);
        vm.startPrank(alice);
        usdc.approve(address(vault), 10e6);
        vm.expectRevert(RIKOVault.RV_OraclePriceInvalid.selector);
        vault.deposit(address(usdc), 10e6, 0, alice);
        vm.stopPrank();
    }

    function testDepositRevertsOnFutureOracleTimestamp() public {
        usdcUsdFeed.setRoundData(1e8, block.timestamp + 1);
        vm.startPrank(alice);
        usdc.approve(address(vault), 10e6);
        vm.expectRevert(RIKOVault.RV_OraclePriceInvalid.selector);
        vault.deposit(address(usdc), 10e6, 0, alice);
        vm.stopPrank();
    }

    function testProcessPendingRedeemReturnsFalseWhileStillUnderfunded() public {
        vm.startPrank(alice);
        usdc.approve(address(vault), 100e6);
        vault.deposit(address(usdc), 50e6, 0, alice);
        vm.stopPrank();

        usdc.transfer(address(0xBEEF), 45e6);
        vm.prank(alice);
        vault.redeem(address(usdc), 50e6, 0, alice);

        (bool completed, uint256 sent) = vault.processPendingRedemption(alice, address(usdc));
        assertFalse(completed, "must wait for custody refill");
        assertEq(sent, 0, "no transfer yet");
    }

    function testProcessPendingRedeemsBatchReturnsZeroWhileUnderfunded() public {
        vm.startPrank(alice);
        usdc.approve(address(vault), 100e6);
        vault.deposit(address(usdc), 50e6, 0, alice);
        vm.stopPrank();

        usdc.transfer(address(0xBEEF), 45e6);
        vm.prank(alice);
        vault.redeem(address(usdc), 50e6, 0, alice);

        (uint256 processed, uint256 totalSent) = vault.processPendingRedemptions(alice, address(usdc), 3);
        assertEq(processed, 0, "batch should not process while underfunded");
        assertEq(totalSent, 0, "no transfer while underfunded");
        (uint256 rikoLocked,,,, bool exists) = vault.pendingRedemptions(alice, address(usdc));
        assertTrue(exists, "pending redemption should remain");
        assertEq(rikoLocked, 50e6, "head should remain unchanged");
    }

    function testProcessPendingRedeemsBatchRevertsOnZeroMaxItems() public {
        vm.expectRevert(RIKOVault.RV_InvalidAmount.selector);
        vault.processPendingRedemptions(alice, address(usdc), 0);
    }

    function testProcessPendingRedeemsBatchOnlyOperator() public {
        vm.startPrank(alice);
        usdc.approve(address(vault), 100e6);
        vault.deposit(address(usdc), 50e6, 0, alice);
        vm.stopPrank();

        usdc.transfer(address(0xBEEF), 45e6);
        vm.prank(alice);
        vault.redeem(address(usdc), 50e6, 0, alice);

        vault.setPendingRedemptionOperator(address(0xB0B));
        vm.prank(alice);
        vm.expectRevert(RIKOVault.RV_PendingRedemptionOperatorOnly.selector);
        vault.processPendingRedemptions(alice, address(usdc), 1);
    }

    function testOnlyOwnerAdminMatrix() public {
        bytes32 feedHash = keccak256(bytes("USDC / USD"));

        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, alice));
        vault.pause();

        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, alice));
        vault.unpause();

        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, alice));
        vault.setGlobalSupplyCapUsd6(1);

        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, alice));
        vault.setTokenTvlCapUsd6(address(usdc), 1);

        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, alice));
        vault.setTokenDepositStorageMode(address(usdc), true);

        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, alice));
        vault.setCustodyAddress(alice);

        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, alice));
        vault.setRikoPriceUsd6(2e6);

        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, alice));
        vault.setPendingRedemptionOperator(alice);

        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, alice));
        vault.setWrappedNativeToken(address(usdc));

        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, alice));
        vault.setTokenConfig(address(usdc), true, address(usdcUsdFeed), 1 days, feedHash);
    }

    function testOwnable2StepTransferAndAcceptFlow() public {
        address newOwner = address(0xB0B);
        vault.transferOwnership(newOwner);

        vm.prank(newOwner);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, newOwner));
        vault.setRikoPriceUsd6(2e6);

        vm.prank(newOwner);
        vault.acceptOwnership();

        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, address(this)));
        vault.setRikoPriceUsd6(2e6);

        vm.prank(newOwner);
        vault.setRikoPriceUsd6(2e6);
        assertEq(vault.rikoPriceUsd6(), 2e6, "new owner should control admin actions");
    }

    function testSetRikoPriceRevertsOnZero() public {
        vm.expectRevert(RIKOVault.RV_InvalidRikoPrice.selector);
        vault.setRikoPriceUsd6(0);
    }

    function testSetPendingRedemptionOperatorRevertsOnZero() public {
        vm.expectRevert(RIKOVault.RV_InvalidPendingRedemptionOperator.selector);
        vault.setPendingRedemptionOperator(address(0));
    }
}
