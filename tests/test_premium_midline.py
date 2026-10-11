"""Regression cases for the signed premium center, with no network or orders."""

import unittest
from decimal import Decimal

from arb_bot.basis import PremiumStrategy
from arb_bot.live_config import LiveConfig
from arb_bot.quotes import Quote


class MidlineTests(unittest.TestCase):
    def signal(self, midline, long_edge, short_edge):
        config = LiveConfig(midline_bps=midline, upper_bps=4, lower_bps=4)
        strategy = PremiumStrategy(["SOL"], config)
        # Var ask and Lighter ask are both 100. Bids set the two independent
        # executable edges, without bypassing the production edge calculation.
        quote = Quote(
            Decimal("100") * (1 + Decimal(str(short_edge)) / 10000),
            Decimal("100"), "test", Decimal("0.1"), 0, 0, None,
        )
        lighter_bid = Decimal("100") * (1 + Decimal(str(long_edge)) / 10000)
        return strategy.tick(1, "SOL", quote, lighter_bid, Decimal("100"))[2]

    def test_positive_center_lowers_long_hurdle(self):
        self.assertIsNone(self.signal(2, 1.9, -10))
        self.assertEqual(self.signal(2, 2, -10), ("long_var", 2))

    def test_positive_center_raises_short_hurdle(self):
        self.assertIsNone(self.signal(2, -10, 5.9))
        self.assertEqual(self.signal(2, -10, 6), ("short_var", 6))

    def test_negative_center_reverses_the_hurdles(self):
        self.assertIsNone(self.signal(-2, 5.9, -10))
        self.assertEqual(self.signal(-2, 6, -10), ("long_var", 6))
        self.assertIsNone(self.signal(-2, -10, 1.9))
        self.assertEqual(self.signal(-2, -10, 2), ("short_var", 2))

    def test_zero_center_keeps_symmetric_band(self):
        self.assertIsNone(self.signal(0, 3.9, 3.9))
        self.assertEqual(self.signal(0, 4, -10), ("long_var", 4))
        self.assertEqual(self.signal(0, -10, 4), ("short_var", 4))


if __name__ == "__main__":
    unittest.main()
