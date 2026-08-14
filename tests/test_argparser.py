from e3sm_to_cmip.argparser import setup_argparser


def test_simple_help_describes_current_behavior():
    help_text = setup_argparser().format_help()

    assert "NOT WORKING" not in help_text
    assert "Bundled tables are used by default" in help_text
    assert "legacy handlers" in help_text
