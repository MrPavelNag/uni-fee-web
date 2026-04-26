// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "forge-std/Test.sol";
import {RIKOVault} from "../contracts/RIKOVault.sol";
import {MockERC20, MockAggregatorV3} from "./mocks/RIKOMocks.sol";

contract RIKOVaultScenarioTest is Test {
    RIKOVault internal vault;
    MockERC20 internal usdc;
    MockERC20 internal weth;
    MockAggregatorV3 internal usdcUsdFeed;
    MockAggregatorV3 internal wethUsdFeed;

    address internal alice = address(0xA11CE);
    address internal bob = address(0xB0B);

    function setUp() public {
        vault = new RIKOVault(address(this));
        vault.setPendingRedemptionOperator(address(this));

        usdc = new MockERC20("USD Coin", "USDC", 6);
        weth = new MockERC20("Wrapped Ether", "WETH", 18);
        usdcUsdFeed = new MockAggregatorV3(8, "USDC / USD", 1e8);
        wethUsdFeed = new MockAggregatorV3(8, "WETH / USD", 2_000e8);

        vault.setTokenConfig(address(usdc), true, address(usdcUsdFeed), 1 days, keccak256(bytes("USDC / USD")));
        vault.setTokenConfig(address(weth), true, address(wethUsdFeed), 1 days, keccak256(bytes("WETH / USD")));

        // Custody is owner (address(this)) by default. Allow vault to pull liquidity on redeem.
        usdc.approve(address(vault), type(uint256).max);
        weth.approve(address(vault), type(uint256).max);
    }

    function testBasketStress_RepriceBlocksTokenCapButOtherAssetStillWorks() public {
        vault.setTokenTvlCapUsd6(address(weth), 50_000e6);

        weth.mint(alice, 21 ether);
        vm.startPrank(alice);
        weth.approve(address(vault), type(uint256).max);
        uint256 firstMint = vault.deposit(address(weth), 20 ether, 0, alice);
        assertEq(firstMint, 40_000e6, "initial WETH mint at 2000 USD");
        vm.stopPrank();

        // Basket stress: WETH price reprices up, so current USD exposure grows.
        wethUsdFeed.setRoundData(3_000e8, block.timestamp);

        vm.startPrank(alice);
        vm.expectRevert(RIKOVault.RV_TokenCapExceeded.selector);
        vault.deposit(address(weth), 1 ether, 0, alice);
        vm.stopPrank();

        // Other basket leg should continue to operate.
        usdc.mint(bob, 10_000e6);
        vm.startPrank(bob);
        usdc.approve(address(vault), type(uint256).max);
        uint256 usdcMint = vault.deposit(address(usdc), 10_000e6, 0, bob);
        vm.stopPrank();
        assertEq(usdcMint, 10_000e6, "USDC leg still works under WETH stress");
    }

    function testBasketStress_PriceDropQueuesRedeemThenSettlesAfterRefill() public {
        weth.mint(alice, 10 ether);
        vm.startPrank(alice);
        weth.approve(address(vault), type(uint256).max);
        uint256 minted = vault.deposit(address(weth), 10 ether, 0, alice);
        vm.stopPrank();
        assertEq(minted, 20_000e6, "minted at 2000 USD");

        // Price drops by 50%: same RIKO now requires 2x WETH on redeem.
        wethUsdFeed.setRoundData(1_000e8, block.timestamp);

        // Simulate custody underfunded for WETH.
        weth.transfer(address(0xBEEF), 8 ether);

        vm.prank(alice);
        uint256 outNow = vault.redeem(address(weth), minted, 0, alice);
        assertEq(outNow, 0, "redeem should be queued when custody is underfunded");

        (uint256 rikoLocked, uint256 queuedTokenOut,,, bool exists) = vault.pendingRedemptions(alice, address(weth));
        assertTrue(exists, "pending redemption exists");
        assertEq(rikoLocked, minted, "full RIKO amount queued");
        assertEq(queuedTokenOut, 20 ether, "queued token out reflects stressed price");

        weth.mint(address(this), 20 ether);
        uint256 aliceBefore = weth.balanceOf(alice);
        (bool completed, uint256 sent) = vault.processPendingRedemption(alice, address(weth));
        assertTrue(completed, "operator settles queued redeem after refill");
        assertEq(sent, 20 ether, "sent equals queued stressed quote");
        assertEq(weth.balanceOf(alice), aliceBefore + 20 ether, "alice receives settled WETH");
    }
}
