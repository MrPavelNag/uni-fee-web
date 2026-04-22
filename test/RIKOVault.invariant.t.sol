// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "forge-std/Test.sol";
import "forge-std/StdInvariant.sol";
import {RIKOVault} from "../contracts/RIKOVault.sol";
import {MockERC20, MockAggregatorV3} from "./mocks/RIKOMocks.sol";

contract RIKOVaultHandler is Test {
    RIKOVault internal immutable vault;
    MockERC20 internal immutable usdc;

    address[] internal actors;

    constructor(RIKOVault _vault, MockERC20 _usdc) {
        vault = _vault;
        usdc = _usdc;
        actors.push(address(0xA1));
        actors.push(address(0xA2));
        actors.push(address(0xA3));
        actors.push(address(0xA4));
    }

    function _actor(uint256 seed) internal view returns (address) {
        return actors[seed % actors.length];
    }

    function deposit(uint256 actorSeed, uint256 amount) external {
        address a = _actor(actorSeed);
        uint256 amt = bound(amount, 1, 50_000e6);
        usdc.mint(a, amt);
        vm.startPrank(a);
        usdc.approve(address(vault), amt);
        vault.deposit(address(usdc), amt, 0, a);
        vm.stopPrank();
    }

    function redeem(uint256 actorSeed, uint256 amount) external {
        address a = _actor(actorSeed);
        uint256 bal = vault.balanceOf(a);
        if (bal == 0) return;
        uint256 amt = bound(amount, 1, bal);
        vm.prank(a);
        vault.redeem(address(usdc), amt, 0, a);
    }
}

contract RIKOVaultInvariantTest is StdInvariant, Test {
    RIKOVault internal vault;
    MockERC20 internal usdc;
    MockAggregatorV3 internal usdcUsdFeed;
    RIKOVaultHandler internal handler;

    uint256 internal constant GLOBAL_CAP = 2_000_000e6;

    function setUp() public {
        vault = new RIKOVault(address(this));
        usdc = new MockERC20("USD Coin", "USDC", 6);
        usdcUsdFeed = new MockAggregatorV3(8, "USDC / USD", 1e8);
        bytes32 feedHash = keccak256(bytes("USDC / USD"));
        vault.setTokenConfig(address(usdc), true, address(usdcUsdFeed), 1 days, feedHash);
        vault.setGlobalSupplyCapUsd6(GLOBAL_CAP);
        vault.setTokenTvlCapUsd6(address(usdc), GLOBAL_CAP);

        handler = new RIKOVaultHandler(vault, usdc);
        targetContract(address(handler));
    }

    function invariant_TotalSupplyNeverExceedsGlobalCap() public view {
        assertLe(vault.totalSupply(), GLOBAL_CAP, "global cap invariant");
    }

    function invariant_FullyBackedInSingleAssetMode() public view {
        // With single whitelisted USDC and 1 USD oracle, minted RIKO should
        // remain fully backed by current vault USDC reserves.
        assertEq(vault.totalSupply(), usdc.balanceOf(address(vault)), "backing invariant");
    }
}
