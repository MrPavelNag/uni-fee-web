// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "forge-std/Test.sol";
import {Pausable} from "@openzeppelin/contracts/utils/Pausable.sol";
import {RIKOVault} from "../contracts/RIKOVault.sol";
import {MockERC20, MockAggregatorV3} from "./mocks/RIKOMocks.sol";

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
        usdc.mint(alice, 1_000_000e6);
    }

    function testSetTokenConfigRevertsOnBadOracleDecimals() public {
        MockAggregatorV3 badFeedZero = new MockAggregatorV3(0, "USDC / USD", 1e8);
        bytes32 feedHash = keccak256(bytes("USDC / USD"));
        vm.expectRevert(RIKOVault.OracleDecimalsInvalid.selector);
        vault.setTokenConfig(address(usdc), true, address(badFeedZero), 1 days, feedHash);

        MockAggregatorV3 badFeedTooHigh = new MockAggregatorV3(19, "USDC / USD", 1e8);
        vm.expectRevert(RIKOVault.OracleDecimalsInvalid.selector);
        vault.setTokenConfig(address(usdc), true, address(badFeedTooHigh), 1 days, feedHash);
    }

    function testSetTokenConfigRevertsOnBadTokenDecimals() public {
        MockERC20 badTokenZero = new MockERC20("BAD", "BAD0", 0);
        bytes32 feedHash = keccak256(bytes("USDC / USD"));
        vm.expectRevert(RIKOVault.TokenDecimalsInvalid.selector);
        vault.setTokenConfig(address(badTokenZero), true, address(usdcUsdFeed), 1 days, feedHash);

        MockERC20 badTokenTooHigh = new MockERC20("BAD", "BAD19", 19);
        vm.expectRevert(RIKOVault.TokenDecimalsInvalid.selector);
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
        vm.expectRevert(RIKOVault.OraclePriceStale.selector);
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

    function testRedeemRevertsOnInsufficientLiquidity() public {
        vm.startPrank(alice);
        usdc.approve(address(vault), 100e6);
        vault.deposit(address(usdc), 50e6, 0, alice);
        vm.stopPrank();

        // Simulate outflow from vault (e.g. external admin/op loss) to test guard.
        vm.prank(address(vault));
        usdc.transfer(address(this), 45e6);

        vm.prank(alice);
        vm.expectRevert(RIKOVault.InsufficientLiquidity.selector);
        vault.redeem(address(usdc), 50e6, 0, alice);
    }

    function testConfigDisableAndReEnable() public {
        bytes32 feedHash = keccak256(bytes("USDC / USD"));
        vault.setTokenConfig(address(usdc), false, address(0), 0, bytes32(0));

        vm.startPrank(alice);
        usdc.approve(address(vault), 50e6);
        vm.expectRevert(RIKOVault.UnsupportedToken.selector);
        vault.deposit(address(usdc), 10e6, 0, alice);
        vm.stopPrank();

        vault.setTokenConfig(address(usdc), true, address(usdcUsdFeed), 1 days, feedHash);

        vm.prank(alice);
        uint256 minted = vault.deposit(address(usdc), 10e6, 0, alice);
        assertEq(minted, 10e6, "must work after re-enable");
    }
}
