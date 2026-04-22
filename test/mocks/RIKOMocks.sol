// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";

contract MockERC20 is ERC20 {
    uint8 private immutable _customDecimals;

    constructor(string memory n, string memory s, uint8 d) ERC20(n, s) {
        _customDecimals = d;
    }

    function decimals() public view override returns (uint8) {
        return _customDecimals;
    }

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }
}

contract MockAggregatorV3 {
    uint8 public immutable decimals;
    string public description;
    int256 internal _answer;
    uint256 internal _updatedAt;

    constructor(uint8 d, string memory desc_, int256 answer_) {
        decimals = d;
        description = desc_;
        _answer = answer_;
        _updatedAt = block.timestamp;
    }

    function setRoundData(int256 answer_, uint256 updatedAt_) external {
        _answer = answer_;
        _updatedAt = updatedAt_;
    }

    function latestRoundData()
        external
        view
        returns (uint80, int256, uint256, uint256, uint80)
    {
        return (1, _answer, _updatedAt, _updatedAt, 1);
    }
}
