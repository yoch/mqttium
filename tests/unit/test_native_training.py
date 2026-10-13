"""The profile-guided build trains on a workload that must keep running."""

from mqttium.codec import _train


def test_training_workload_runs_on_the_selected_backend():
    _train.main(rounds=3)
