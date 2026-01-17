import os
import unittest
from unittest.mock import patch

from utils.env import get_env, reload_env


class TestEnvFallback(unittest.TestCase):
    def setUp(self):
        # システム環境変数をセット
        os.environ["TEST_FALLBACK_VAR"] = "system_value"
        if "TEST_ENV_VAR" in os.environ:
            del os.environ["TEST_ENV_VAR"]

    def test_get_env_fallback_to_system_when_override_enabled(self):
        # .env の内容をモック (PAL_MCP_FORCE_ENV_OVERRIDE=true, TEST_FALLBACK_VAR は定義なし)
        mock_dotenv = {"PAL_MCP_FORCE_ENV_OVERRIDE": "true", "OTHER_VAR": "other_value"}

        with patch("utils.env._read_dotenv_values", return_value=mock_dotenv):
            reload_env()

            # .env にない変数はシステム環境変数から取得されるべき
            value = get_env("TEST_FALLBACK_VAR")
            self.assertEqual(value, "system_value")

            # .env にある変数は .env から取得されるべき
            value = get_env("OTHER_VAR")
            self.assertEqual(value, "other_value")

    def test_get_env_prefers_dotenv_when_override_enabled(self):
        # 両方に存在する変数の場合
        os.environ["CONFLICT_VAR"] = "system_value"
        mock_dotenv = {"PAL_MCP_FORCE_ENV_OVERRIDE": "true", "CONFLICT_VAR": "dotenv_value"}

        with patch("utils.env._read_dotenv_values", return_value=mock_dotenv):
            reload_env()

            # .env の値が優先されるべき
            value = get_env("CONFLICT_VAR")
            self.assertEqual(value, "dotenv_value")

    def test_get_env_prefers_system_when_override_disabled(self):
        # オーバーライド無効の場合
        os.environ["CONFLICT_VAR"] = "system_value"
        mock_dotenv = {"PAL_MCP_FORCE_ENV_OVERRIDE": "false", "CONFLICT_VAR": "dotenv_value"}

        with patch("utils.env._read_dotenv_values", return_value=mock_dotenv):
            reload_env()

            # システム環境変数が優先されるべき (os.getenv の標準挙動)
            value = get_env("CONFLICT_VAR")
            self.assertEqual(value, "system_value")


if __name__ == "__main__":
    unittest.main()
