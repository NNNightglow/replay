import polars as pl

from utils.metadata.sector_data_manager import SectorDataManager


def test_calculate_indicators_preserve_text_columns_types():
    # 构造最小数据样本，包含文本列与数值列
    df = pl.DataFrame({
        '日期': ['2025-10-10', '2025-10-11'],
        '开盘': [10.0, 10.5],
        '收盘': [10.2, 10.7],
        '最高': [10.3, 10.9],
        '最低': [9.9, 10.4],
        '成交量': [1000.0, 1200.0],
        '成交额': [1.0, 1.2],
        '板块名称': ['测试板块', '测试板块'],
        '板块类型': ['概念', '概念'],
        '板块代码': ['BK0001', 'BK0001'],
        '数据源': ['同花顺', '同花顺'],
    })

    mgr = SectorDataManager()
    result = mgr._calculate_technical_indicators(df)

    # 文本列应保持为Utf8
    for col in ['板块名称', '板块类型', '板块代码', '数据源']:
        assert col in result.columns
        assert result[col].dtype == pl.Utf8

    # 数值指标列存在且为Float/Int
    numeric_float_cols = ['涨跌幅', '振幅', '换手率', '5日涨跌幅', '10日涨跌幅', 'MA5', 'MA10', 'MA20', '成交额量比']
    for col in numeric_float_cols:
        assert col in result.columns
        assert result[col].dtype == pl.Float64

    assert '连阳天数' in result.columns
    assert result['连阳天数'].dtype in (pl.Int64, pl.Int32)


