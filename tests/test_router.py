from src.main import choose_engine


def test_small_file_uses_python_batch(monkeypatch):

    monkeypatch.setattr(
        "src.main.os.path.exists",
        lambda path: True
    )

    monkeypatch.setattr(
        "src.main.os.path.getsize",
        lambda path: 50 * 1024 * 1024
    )

    engine, size_mb, reason = choose_engine(
        "small.csv"
    )

    assert engine == "python_batch"
    assert size_mb == 50
    assert "<= 200 MB" in reason


def test_big_file_uses_pyspark(monkeypatch):

    monkeypatch.setattr(
        "src.main.os.path.exists",
        lambda path: True
    )

    monkeypatch.setattr(
        "src.main.os.path.getsize",
        lambda path: 500 * 1024 * 1024
    )

    engine, size_mb, reason = choose_engine(
        "big.csv"
    )

    assert engine == "pyspark"
    assert size_mb == 500
    assert "> 200 MB" in reason
