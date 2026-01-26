
import json
import time
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch
import pytest
from monitor.coordinator import InstanceTracker, utc_now

class TestMonitorRootDuration:
    @pytest.fixture
    def tracker(self):
        return InstanceTracker("test_instance", uptime_seconds=0.0)

    def test_primary_tool_tracking(self, tracker):
        """ルートツール(clink)が開始されたとき、is_primaryフラグによって固定されることを確認"""
        start_time = datetime(2026, 1, 26, 12, 0, 0, tzinfo=timezone.utc)
        
        with patch('monitor.coordinator.utc_now', return_value=start_time):
            tracker.start_tool("clink", "{}", is_primary=True)
        
        assert tracker.primary_tool == "clink"
        assert tracker.primary_tool_start_time == start_time
        
        # サブツールが開始されても primary_tool は維持されるべき
        mid_time = start_time + timedelta(seconds=1)
        with patch('monitor.coordinator.utc_now', return_value=mid_time):
            tracker.start_tool("Bash", "{}", is_primary=False)
            
        assert tracker.primary_tool == "clink"  # 上書きされていない
        assert tracker.active_tool == "Bash"   # active_toolは更新される
        assert tracker.primary_tool_start_time == start_time # 開始時刻も維持

    def test_duration_calculation_on_end(self, tracker):
        """セッション終了時にルートツールの実行時間が正しく計算・記録されることを確認"""
        start_time = datetime(2026, 1, 26, 12, 0, 0, tzinfo=timezone.utc)
        end_time = start_time + timedelta(seconds=5, milliseconds=500) # 5.5秒
        
        with patch('monitor.coordinator.utc_now', return_value=start_time):
            tracker.start_tool("clink", "{}", is_primary=True)
            
        # ロガーをモックして出力を確認
        with patch('monitor.coordinator.logger') as mock_logger:
            with patch('monitor.coordinator.utc_now', return_value=end_time):
                # clinkを終了（=セッション終了）
                tracker.end_tool("clink", duration_ms=5500, is_error=False)
                
            # [Metrics] ログが出力されているか
            # infoの呼び出しを確認。引数に 5.50s が含まれているはず
            mock_logger.info.assert_any_call("[Metrics] Root tool 'clink' finished in 5.50s")
            
        # 終了後はクリアされていること
        assert tracker.primary_tool is None
        assert tracker.primary_tool_start_time is None

    def test_nested_tool_completion_logic(self, tracker):
        """サブツールが終了しても、ルートツールが終わるまでセッション時間は確定しないことを確認"""
        start_time = datetime(2026, 1, 26, 12, 0, 0, tzinfo=timezone.utc)
        
        with patch('monitor.coordinator.utc_now', return_value=start_time):
            tracker.start_tool("clink", "{}", is_primary=True)
            tracker.start_tool("Bash", "{}", is_primary=False)
            
        mid_time = start_time + timedelta(seconds=2)
        with patch('monitor.coordinator.logger') as mock_logger:
            with patch('monitor.coordinator.utc_now', return_value=mid_time):
                # Bashだけ終了
                tracker.end_tool("Bash", duration_ms=1000, is_error=False)
                
            # まだ clink が動いているので、Metricsログは出ないはず
            for call in mock_logger.info.call_args_list:
                assert "[Metrics]" not in call[0][0]
            
            assert tracker.primary_tool == "clink" # まだ維持
            
    def test_pal_root_priority(self, tracker):
        """is_primaryフラグによって、汎用ツール(Read等)よりもPALルートツールが優先されることを確認"""
        # 1. まず Read (agent tool) が開始される（イレギュラーだが起こりうる）
        tracker.start_tool("Read", "{}", is_primary=False)
        assert tracker.primary_tool == "Read"
        assert tracker._primary_locked is False
        
        # 2. 次に clink (PAL root tool) が is_primary=True で開始される -> 上書きされるべき
        tracker.start_tool("clink", "{}", is_primary=True)
        assert tracker.primary_tool == "clink"
        assert tracker._primary_locked is True
        
        # 3. その後 Write (agent tool) が is_primary=False で開始される -> 上書きされないべき
        tracker.start_tool("Write", "{}", is_primary=False)
        assert tracker.primary_tool == "clink"
        assert tracker._primary_locked is True
