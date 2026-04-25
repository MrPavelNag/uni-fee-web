// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "forge-std/Test.sol";
import {RIKOYieldDistributor} from "../contracts/RIKOYieldDistributor.sol";
import {MockERC20} from "./mocks/RIKOMocks.sol";

contract RIKOYieldDistributorTest is Test {
    RIKOYieldDistributor internal distributor;
    MockERC20 internal riko;
    MockERC20 internal usdc;

    address internal alice = address(0xA11CE);
    address internal bob = address(0xB0B);

    function setUp() public {
        riko = new MockERC20("RIKO", "RIKO", 6);
        usdc = new MockERC20("USD Coin", "USDC", 6);
        distributor = new RIKOYieldDistributor(address(this), address(riko));
        distributor.setYieldTokenAddress(address(usdc));
        distributor.setYieldPayerAddress(address(this));
        usdc.approve(address(distributor), type(uint256).max);
    }

    function testDistributeYieldBatchPaysByRikoBalance() public {
        riko.mint(alice, 100e6);
        riko.mint(bob, 50e6);
        usdc.mint(address(this), 1_000_000e6);

        distributor.setMonthlyYieldRateBps(100); // 1%
        uint256 cycle = distributor.currentYieldCycle();

        uint256 beforeBal = usdc.balanceOf(alice);
        uint256 beforeBobBal = usdc.balanceOf(bob);
        address[] memory holders = new address[](2);
        holders[0] = alice;
        holders[1] = bob;
        uint256 payout = distributor.distributeYieldBatch(cycle, holders);
        uint256 afterBal = usdc.balanceOf(alice);
        uint256 afterBobBal = usdc.balanceOf(bob);

        assertEq(afterBal - beforeBal, 1e6, "alice receives 1%");
        assertEq(afterBobBal - beforeBobBal, 500_000, "bob receives 1%");
        assertEq(payout, 1.5e6, "batch total payout");
    }

    function testDistributeYieldBatchSkipsAlreadyPaidAccountInSameCycle() public {
        riko.mint(alice, 100e6);
        usdc.mint(address(this), 1_000_000e6);
        distributor.setMonthlyYieldRateBps(100);
        uint256 cycle = distributor.currentYieldCycle();
        address[] memory holders = new address[](1);
        holders[0] = alice;

        distributor.distributeYieldBatch(cycle, holders);
        uint256 afterFirst = usdc.balanceOf(alice);
        distributor.distributeYieldBatch(cycle, holders);
        uint256 afterSecond = usdc.balanceOf(alice);
        assertEq(afterFirst, afterSecond, "second pass in same cycle should not repay");
    }

    function testClaimFunctionsAreDisabled() public {
        riko.mint(alice, 100e6);
        distributor.setMonthlyYieldRateBps(100);

        vm.prank(alice);
        vm.expectRevert(RIKOYieldDistributor.RYD_ClaimDisabled.selector);
        distributor.claimMonthlyYield();
    }

    function testDistributeUsesRateStoredPerCycle() public {
        riko.mint(alice, 100e6);
        usdc.mint(address(this), 1_000_000e6);

        distributor.setMonthlyYieldRateBps(100); // cycle1 = 1%
        uint256 cycle1 = distributor.currentYieldCycle();
        distributor.setMonthlyYieldRateBps(200); // cycle2 = 2%
        uint256 cycle2 = distributor.currentYieldCycle();

        address[] memory holders = new address[](1);
        holders[0] = alice;

        distributor.distributeYieldBatch(cycle1, holders);
        uint256 afterCycle1 = usdc.balanceOf(alice);
        assertEq(afterCycle1, 1e6, "cycle1 should pay by historical 1% bps");

        distributor.distributeYieldBatch(cycle2, holders);
        uint256 afterCycle2 = usdc.balanceOf(alice);
        assertEq(afterCycle2 - afterCycle1, 2e6, "cycle2 should pay by 2% bps");
    }

    function testDistributeRevertsForInvalidCycle() public {
        vm.expectRevert(RIKOYieldDistributor.RYD_InvalidCycle.selector);
        address[] memory holders = new address[](1);
        holders[0] = alice;
        distributor.distributeYieldBatch(1, holders);
    }

    function testClaimMonthlyYieldForIsDisabled() public {
        vm.expectRevert(RIKOYieldDistributor.RYD_ClaimDisabled.selector);
        distributor.claimMonthlyYieldFor(alice);
    }
}
