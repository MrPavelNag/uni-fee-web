// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {Script, console2} from "forge-std/Script.sol";
import {RIKOVault} from "../contracts/RIKOVault.sol";

contract DeployRIKOStack is Script {
    error EmptyAdminAddress();

    function run() external returns (RIKOVault vault) {
        address admin = vm.envAddress("RIKO_ADMIN");
        if (admin == address(0)) revert EmptyAdminAddress();

        uint256 privateKey = vm.envUint("DEPLOYER_PRIVATE_KEY");

        vm.startBroadcast(privateKey);
        vault = new RIKOVault(admin);
        vm.stopBroadcast();

        console2.log("RIKOVault deployed at:", address(vault));
        console2.log("Vault owner:", vault.owner());
    }
}
