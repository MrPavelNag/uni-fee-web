// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "forge-std/Test.sol";
import {RIKOVault} from "../contracts/RIKOVault.sol";
import {MockERC20, MockAggregatorV3} from "./mocks/RIKOMocks.sol";

contract RIKOVaultFuzzTest is Test {
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
        usdc.approve(address(vault), type(uint256).max);
    }

    function testFuzz_DepositMintsExpectedAmount(uint96 amount) public {
        uint256 depositAmount = bound(uint256(amount), 1, 1_000_000e6);
        usdc.mint(alice, depositAmount);

        vm.startPrank(alice);
        usdc.approve(address(vault), depositAmount);
        uint256 minted = vault.deposit(address(usdc), depositAmount, 0, alice);
        vm.stopPrank();

        assertEq(minted, depositAmount, "RIKO mint amount");
        assertEq(vault.balanceOf(alice), depositAmount, "RIKO balance");
        assertEq(usdc.balanceOf(address(vault)), 0, "vault token balance");
    }

    function testFuzz_DepositRespectsMinOut(uint96 amount, uint96 minOutBump) public {
        uint256 depositAmount = bound(uint256(amount), 1, 100_000e6);
        uint256 minOut = depositAmount + bound(uint256(minOutBump), 1, 1_000_000);
        usdc.mint(alice, depositAmount);

        vm.startPrank(alice);
        usdc.approve(address(vault), depositAmount);
        vm.expectRevert(RIKOVault.RV_SlippageExceeded.selector);
        vault.deposit(address(usdc), depositAmount, minOut, alice);
        vm.stopPrank();
    }

    function testFuzz_RedeemRoundTrip(uint96 amount, uint96 redeemPart) public {
        uint256 depositAmount = bound(uint256(amount), 1, 1_000_000e6);
        vault.setTokenDepositStorageMode(address(usdc), true);
        usdc.mint(alice, depositAmount);

        vm.startPrank(alice);
        usdc.approve(address(vault), depositAmount);
        vault.deposit(address(usdc), depositAmount, 0, alice);
        uint256 redeemAmount = bound(uint256(redeemPart), 1, depositAmount);
        uint256 out = vault.redeem(address(usdc), redeemAmount, 0, alice);
        vm.stopPrank();

        assertEq(out, redeemAmount, "redeem amount");
        assertEq(vault.balanceOf(alice), depositAmount - redeemAmount, "remaining RIKO");
    }

    function testFuzz_OracleStaleBlocksActions(uint96 amount) public {
        uint256 depositAmount = bound(uint256(amount), 1, 1_000_000e6);
        usdc.mint(alice, depositAmount);
        // updatedAt = 0 triggers stale branch without timestamp arithmetic.
        usdcUsdFeed.setRoundData(1e8, 0);

        vm.startPrank(alice);
        usdc.approve(address(vault), depositAmount);
        vm.expectRevert(RIKOVault.RV_OraclePriceStale.selector);
        vault.deposit(address(usdc), depositAmount, 0, alice);
        vm.stopPrank();
    }
}
