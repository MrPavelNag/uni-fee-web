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

contract MockMutableDecimalsERC20 is ERC20 {
    uint8 private _customDecimals;

    constructor(string memory n, string memory s, uint8 d) ERC20(n, s) {
        _customDecimals = d;
    }

    function decimals() public view override returns (uint8) {
        return _customDecimals;
    }

    function setDecimals(uint8 d) external {
        _customDecimals = d;
    }

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }
}

contract MockFeeOnTransferERC20 is ERC20 {
    uint8 private immutable _customDecimals;
    uint256 public constant FEE_BPS = 100; // 1%

    constructor(string memory n, string memory s, uint8 d) ERC20(n, s) {
        _customDecimals = d;
    }

    function decimals() public view override returns (uint8) {
        return _customDecimals;
    }

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }

    function _update(address from, address to, uint256 value) internal override {
        if (from == address(0) || to == address(0)) {
            super._update(from, to, value);
            return;
        }

        uint256 fee = (value * FEE_BPS) / 10_000;
        uint256 net = value - fee;
        super._update(from, to, net);
        if (fee > 0) {
            super._update(from, address(0), fee);
        }
    }
}

contract MockAggregatorV3 {
    uint8 public immutable decimals;
    string public description;
    int256 internal _answer;
    uint256 internal _updatedAt;
    uint256 internal _startedAt;
    uint80 internal _roundId = 1;
    uint80 internal _answeredInRound = 1;

    constructor(uint8 d, string memory desc_, int256 answer_) {
        decimals = d;
        description = desc_;
        _answer = answer_;
        _updatedAt = block.timestamp;
        _startedAt = block.timestamp;
    }

    function setRoundData(int256 answer_, uint256 updatedAt_) external {
        _answer = answer_;
        _updatedAt = updatedAt_;
        _startedAt = updatedAt_;
        _roundId = 1;
        _answeredInRound = 1;
    }

    function setRoundMeta(uint80 roundId_, uint80 answeredInRound_, uint256 startedAt_, uint256 updatedAt_) external {
        _roundId = roundId_;
        _answeredInRound = answeredInRound_;
        _startedAt = startedAt_;
        _updatedAt = updatedAt_;
    }

    function latestRoundData()
        external
        view
        returns (uint80, int256, uint256, uint256, uint80)
    {
        return (_roundId, _answer, _startedAt, _updatedAt, _answeredInRound);
    }
}
