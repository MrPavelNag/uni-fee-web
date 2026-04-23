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

    function setUp() public {
        riko = new MockERC20("RIKO", "RIKO", 6);
        usdc = new MockERC20("USD Coin", "USDC", 6);
        distributor = new RIKOYieldDistributor(address(this), address(riko));
        distributor.setYieldTokenAddress(address(usdc));
        distributor.setYieldPayerAddress(address(this));
        usdc.approve(address(distributor), type(uint256).max);
    }

    function testClaimMonthlyYieldPaysByRikoBalance() public {
        riko.mint(alice, 100e6);
        usdc.mint(address(this), 1_000_000e6);

        distributor.setMonthlyYieldRateBps(100); // 1%

        uint256 beforeBal = usdc.balanceOf(alice);
        vm.prank(alice);
        uint256 payout = distributor.claimMonthlyYield();
        uint256 afterBal = usdc.balanceOf(alice);

        assertEq(payout, 1e6, "payout should equal 1% of RIKO balance");
        assertEq(afterBal - beforeBal, payout, "alice receives payout");
    }

    function testClaimRevertsOnSecondClaimInSameCycle() public {
        riko.mint(alice, 100e6);
        usdc.mint(address(this), 1_000_000e6);
        distributor.setMonthlyYieldRateBps(100);

        vm.prank(alice);
        distributor.claimMonthlyYield();
        vm.prank(alice);
        vm.expectRevert(RIKOYieldDistributor.RYD_YieldNotReady.selector);
        distributor.claimMonthlyYield();
    }

    function testClaimMonthlyYieldForRevertsOnAccountMismatch() public {
        riko.mint(alice, 100e6);
        distributor.setMonthlyYieldRateBps(100);

        vm.prank(address(0xB0B));
        vm.expectRevert(RIKOYieldDistributor.RYD_YieldAccountMismatch.selector);
        distributor.claimMonthlyYieldFor(alice);
    }
}
