// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "forge-std/Test.sol";
import {RIKOVault} from "../contracts/RIKOVault.sol";
import {MockERC20, MockWETH, MockAggregatorV3} from "./mocks/RIKOMocks.sol";

contract RIKOVaultTest is Test {
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

    function testSetTokenConfigRevertsOnFeedMismatch() public {
        bytes32 wrongHash = keccak256(bytes("ETH / USD"));
        vm.expectRevert(RIKOVault.RV_OracleFeedMismatch.selector);
        vault.setTokenConfig(address(usdc), true, address(usdcUsdFeed), 1 days, wrongHash);
    }

    function testDepositMintsRikoAtOneToOneUsd() public {
        uint256 depositAmount = 100e6; // 100 USDC

        vm.startPrank(alice);
        usdc.approve(address(vault), depositAmount);
        uint256 rikoMinted = vault.deposit(address(usdc), depositAmount, 0, alice);
        vm.stopPrank();

        assertEq(rikoMinted, 100e6, "minted RIKO should equal USD6 value");
        assertEq(vault.balanceOf(alice), 100e6, "alice RIKO balance");
        assertEq(usdc.balanceOf(address(vault)), 0, "vault keeps no idle liquidity");
        assertEq(usdc.balanceOf(address(this)), depositAmount, "custody receives deposited funds");
    }

    function testRedeemBurnsRikoAndReturnsToken() public {
        uint256 depositAmount = 50e6;

        vm.startPrank(alice);
        usdc.approve(address(vault), depositAmount);
        vault.deposit(address(usdc), depositAmount, 0, alice);
        uint256 tokenOut = vault.redeem(address(usdc), 20e6, 0, alice);
        vm.stopPrank();

        assertEq(tokenOut, 20e6, "redeem out amount");
        assertEq(vault.balanceOf(alice), 30e6, "remaining RIKO");
        assertEq(usdc.balanceOf(alice), 1_000_000e6 - depositAmount + tokenOut, "alice token balance");
    }

    function testGlobalCapBlocksExcessMint() public {
        vault.setGlobalSupplyCapUsd6(60e6);

        vm.startPrank(alice);
        usdc.approve(address(vault), 100e6);
        vault.deposit(address(usdc), 50e6, 0, alice);
        vm.expectRevert(RIKOVault.RV_GlobalCapExceeded.selector);
        vault.deposit(address(usdc), 20e6, 0, alice);
        vm.stopPrank();
    }

    function testTokenCapBlocksExcessTokenTvl() public {
        vault.setTokenTvlCapUsd6(address(usdc), 40e6);

        vm.startPrank(alice);
        usdc.approve(address(vault), 100e6);
        vm.expectRevert(RIKOVault.RV_TokenCapExceeded.selector);
        vault.deposit(address(usdc), 50e6, 0, alice);
        vm.stopPrank();
    }

    function testProcessPendingRedeemCompletesWhenCustodyRefilled() public {
        vm.startPrank(alice);
        usdc.approve(address(vault), 100e6);
        vault.deposit(address(usdc), 50e6, 0, alice);
        vm.stopPrank();

        usdc.transfer(address(0xBEEF), 45e6);

        vm.prank(alice);
        uint256 first = vault.redeem(address(usdc), 50e6, 0, alice);
        assertEq(first, 0, "first redeem must queue");

        usdc.mint(address(this), 100e6);
        (bool completedBefore, uint256 sentBefore) = vault.processPendingRedemption(alice, address(usdc));
        assertTrue(completedBefore, "pending redeem completed");
        assertEq(sentBefore, 50e6, "token sent");
        assertEq(vault.balanceOf(alice), 0, "riko burned after completion");
    }

    function testRikoPriceAffectsMintAndRedeem() public {
        vault.setRikoPriceUsd6(2e6); // 1 RIKO = 2 USD

        vm.startPrank(alice);
        usdc.approve(address(vault), 100e6);
        uint256 minted = vault.deposit(address(usdc), 100e6, 0, alice);
        assertEq(minted, 50e6, "mint should follow configurable RIKO price");

        uint256 out = vault.redeem(address(usdc), 50e6, 0, alice);
        vm.stopPrank();
        assertEq(out, 100e6, "redeem should follow configurable RIKO price");
    }

    function testProcessPendingRedemptionOnlyOperator() public {
        vault.setPendingRedemptionOperator(address(0xB0B));
        vm.startPrank(alice);
        usdc.approve(address(vault), 100e6);
        vault.deposit(address(usdc), 50e6, 0, alice);
        vm.stopPrank();
        usdc.transfer(address(0xBEEF), 45e6);
        vm.prank(alice);
        vault.redeem(address(usdc), 50e6, 0, alice);

        vm.expectRevert(RIKOVault.RV_PendingRedemptionOperatorOnly.selector);
        vault.processPendingRedemption(alice, address(usdc));
    }

    function testRedeemWrappedNativeSendsNativeEth() public {
        MockWETH weth = new MockWETH();
        MockAggregatorV3 wethUsdFeed = new MockAggregatorV3(8, "WETH / USD", 2_000e8);
        bytes32 feedHash = keccak256(bytes("WETH / USD"));
        vault.setTokenConfig(address(weth), true, address(wethUsdFeed), 1 days, feedHash);
        vault.setWrappedNativeToken(address(weth));

        vm.deal(address(weth), 5 ether);
        weth.mint(alice, 1 ether);
        weth.approve(address(vault), type(uint256).max);
        uint256 rikoIn = vault.quoteDeposit(address(weth), 1 ether);

        vm.startPrank(alice);
        weth.approve(address(vault), type(uint256).max);
        vault.deposit(address(weth), 1 ether, 0, alice);
        uint256 ethBefore = alice.balance;
        uint256 expectedOut = vault.quoteRedeem(address(weth), rikoIn);
        uint256 tokenOut = vault.redeem(address(weth), rikoIn, 0, alice);
        vm.stopPrank();

        assertEq(tokenOut, expectedOut, "tokenOut should match quote");
        assertEq(alice.balance, ethBefore + expectedOut, "alice receives native ETH");
        assertEq(weth.balanceOf(alice), 0, "alice should not receive WETH");
    }

    function testProcessPendingWrappedNativeSendsNativeEth() public {
        MockWETH weth = new MockWETH();
        MockAggregatorV3 wethUsdFeed = new MockAggregatorV3(8, "WETH / USD", 2_000e8);
        bytes32 feedHash = keccak256(bytes("WETH / USD"));
        vault.setTokenConfig(address(weth), true, address(wethUsdFeed), 1 days, feedHash);
        vault.setWrappedNativeToken(address(weth));

        vm.deal(address(weth), 10 ether);
        weth.mint(alice, 1 ether);

        vm.startPrank(alice);
        weth.approve(address(vault), type(uint256).max);
        vault.deposit(address(weth), 1 ether, 0, alice);
        vm.stopPrank();

        // Make immediate settlement impossible: drain custody WETH.
        weth.transfer(address(0xBEEF), weth.balanceOf(address(this)));
        uint256 aliceRiko = vault.balanceOf(alice);

        vm.prank(alice);
        uint256 queued = vault.redeem(address(weth), aliceRiko, 0, alice);
        assertEq(queued, 0, "redeem should queue while underfunded");

        uint256 rikoLocked;
        uint256 minTokenOut;
        bool exists;
        (rikoLocked,, minTokenOut,, exists) = vault.pendingRedemptions(alice, address(weth));
        assertTrue(exists, "pending redemption exists");
        uint256 expectedOut = vault.quoteRedeem(address(weth), rikoLocked);

        weth.mint(address(this), expectedOut);
        weth.approve(address(vault), type(uint256).max);
        vm.deal(address(weth), expectedOut);
        uint256 ethBefore = alice.balance;
        (bool completed, uint256 sent) = vault.processPendingRedemption(alice, address(weth));

        assertTrue(completed, "pending redeem completed");
        assertEq(sent, expectedOut, "sent amount should match quote");
        assertEq(alice.balance, ethBefore + expectedOut, "alice receives native ETH");
        assertEq(minTokenOut, 0, "min out from queued redeem");
    }
}
