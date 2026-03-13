-- 回测记录表重新设计
-- 每只股票只保存 Top 3 最佳因子组合

DROP TABLE IF EXISTS best_factor_combinations;

CREATE TABLE best_factor_combinations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    combination_code VARCHAR(100) UNIQUE NOT NULL,  -- 组合唯一标识
    stock_code VARCHAR(10) NOT NULL,                 -- 股票代码
    stock_name VARCHAR(50),                          -- 股票名称
    
    -- 因子组合
    strategy_desc VARCHAR(200),                      -- 策略描述 "MA(10,20)+RSI(80)(OR)"
    factor_combination TEXT,                         -- JSON: {"ma_5_20": 1, "period_6m": 1}
    active_factors TEXT,                             -- 启用的因子列表
    
    -- 回测结果
    rank_in_stock INTEGER DEFAULT 1,                 -- 该股票内的排名 (1/2/3)
    total_return FLOAT,                              -- 总收益率%
    sharpe_ratio FLOAT,                              -- 夏普比率
    max_drawdown FLOAT,                              -- 最大回撤%
    win_rate FLOAT,                                  -- 胜率%
    trade_count INTEGER,                             -- 交易次数
    composite_score FLOAT,                           -- 综合得分
    
    -- 时间因子
    holding_period VARCHAR(20),                      -- 持仓周期
    start_date DATE,                                 -- 回测开始日期
    end_date DATE,                                   -- 回测结束日期
    
    -- 元数据
    backtest_date DATE,                              -- 回测执行日期
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT 1,                     -- 是否有效
    notes TEXT                                        -- 备注
);

-- 索引
CREATE INDEX idx_stock_rank ON best_factor_combinations(stock_code, rank_in_stock);
CREATE INDEX idx_score ON best_factor_combinations(composite_score DESC);

-- 查询示例
-- 获取所有股票的最佳组合（每只股票top3）
SELECT * FROM best_factor_combinations 
WHERE is_active = 1 
ORDER BY stock_code, rank_in_stock;

-- 获取全市场最佳组合（按综合得分）
SELECT * FROM best_factor_combinations 
WHERE is_active = 1 
ORDER BY composite_score DESC 
LIMIT 100;