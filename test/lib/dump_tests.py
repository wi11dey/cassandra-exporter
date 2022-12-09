import unittest

from lib.dump import MetricsDump


class Tests(unittest.TestCase):
    def test(self):
        dump1 = MetricsDump.from_str("""
# the following are duplicate families
test_family_d {abc="123"} 0 0
test_family_d {abc="456"} 0 0
""")

        dump2 = MetricsDump.from_str("""
# the following are duplicate families
# TYPE test_family_d counter
test_family_d {abc="123"} 0 0
test_family_d {abc="456"} 0 0
""")

        pass


class ValidationTests(unittest.TestCase):
    #     def test_invalid_input(self):
    #         """
    #         Test the
    #         """
    #         data = """
    # busted busted busted
    #         """
    #
    #         with self.assertRaises(ValueError):
    #             metric_dump_tool.MetricsDump.from_lines(data)

    def test_duplicate_families(self):
        """
        Test that validation finds duplicated metric families
        """
        dump = MetricsDump.from_str("""
# TYPE test_family_a counter
test_family_a {} 1234 1234

test_family_b {} 0 0

# TYPE test_family_a gauge
test_family_a {} 5678 1234

# the following are duplicate samples, not duplicate families
# TYPE test_family_c gauge
test_family_c {} 1234 1234
test_family_c {} 1234 1234

# the following are duplicate families
test_family_d {abc="123"} 0 0
test_family_d {abc="456"} 0 0
        """)

        result = dump.validate()

        self.assertIn('test_family_a', result.duplicate_families)
        self.assertIn('test_family_d', result.duplicate_families)
        self.assertNotIn('test_family_b', result.duplicate_families)
        self.assertNotIn('test_family_c', result.duplicate_families)

    def test_duplicate_samples(self):
        """
        Test that validation finds duplicated metric families
        """
        dump = MetricsDump.from_lines("""
# TYPE test_family_a gauge
test_family_a {hello="world"} 1234 1234
test_family_a {hello="world"} 1234 1234
            """)

        result = dump.validate()

        self.assertIn('test_family_a', result.duplicate_families)
        self.assertNotIn('test_family_b', result.duplicate_families)


class DiffTests(unittest.TestCase):
    def test_added_families(self):
        from_dump = MetricsDump.from_lines("""
test_family_a {hello="world"} 0 0
        """)

        to_dump = MetricsDump.from_lines("""
test_family_a {hello="world"} 0 0
test_family_a {hello="universe"} 0 0

test_family_b {} 0 0
        """)

        result = from_dump.diff(to_dump)

        self.assertIn('test_family_b', result.added_families)
        self.assertNotIn('test_family_a', result.added_families)

    def test_removed_families(self):
        from_dump = MetricsDump.from_lines("""
test_family_a {hello="world"} 0 0
test_family_a {hello="universe"} 0 0

test_family_b {} 0 0
        """)

        to_dump = MetricsDump.from_lines("""
test_family_a {hello="world"} 0 0
        """)

        result = from_dump.diff(to_dump)

        self.assertIn('test_family_b', result.removed_families)
        self.assertNotIn('test_family_a', result.removed_families)