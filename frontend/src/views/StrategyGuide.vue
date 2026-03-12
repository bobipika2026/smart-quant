<template>
  <div class="strategy-guide">
    <n-card title="📚 策略指标说明">
      <n-alert type="info" style="margin-bottom: 16px">
        以下是28个内置策略的详细说明，帮助你了解每个策略的原理和适用场景。
      </n-alert>

      <n-tabs type="line" animated>
        <!-- 趋势类策略 -->
        <n-tab-pane name="trend" tab="📈 趋势类 (13个)">
          <n-collapse>
            <n-collapse-item v-for="s in trendStrategies" :key="s.id" :title="s.name" :name="s.id">
              <n-descriptions :column="1" bordered>
                <n-descriptions-item label="策略ID">
                  <n-tag>{{ s.id }}</n-tag>
                </n-descriptions-item>
                <n-descriptions-item label="原理">
                  {{ s.principle }}
                </n-descriptions-item>
                <n-descriptions-item label="买入信号">
                  <n-text type="success">{{ s.buySignal }}</n-text>
                </n-descriptions-item>
                <n-descriptions-item label="卖出信号">
                  <n-text type="error">{{ s.sellSignal }}</n-text>
                </n-descriptions-item>
                <n-descriptions-item label="适用场景">
                  {{ s.scene }}
                </n-descriptions-item>
                <n-descriptions-item label="特点">
                  {{ s.feature }}
                </n-descriptions-item>
              </n-descriptions>
            </n-collapse-item>
          </n-collapse>
        </n-tab-pane>

        <!-- 震荡类策略 -->
        <n-tab-pane name="oscillator" tab="📊 震荡类 (7个)">
          <n-collapse>
            <n-collapse-item v-for="s in oscillatorStrategies" :key="s.id" :title="s.name" :name="s.id">
              <n-descriptions :column="1" bordered>
                <n-descriptions-item label="策略ID">
                  <n-tag>{{ s.id }}</n-tag>
                </n-descriptions-item>
                <n-descriptions-item label="原理">
                  {{ s.principle }}
                </n-descriptions-item>
                <n-descriptions-item label="买入信号">
                  <n-text type="success">{{ s.buySignal }}</n-text>
                </n-descriptions-item>
                <n-descriptions-item label="卖出信号">
                  <n-text type="error">{{ s.sellSignal }}</n-text>
                </n-descriptions-item>
                <n-descriptions-item label="适用场景">
                  {{ s.scene }}
                </n-descriptions-item>
                <n-descriptions-item label="特点">
                  {{ s.feature }}
                </n-descriptions-item>
              </n-descriptions>
            </n-collapse-item>
          </n-collapse>
        </n-tab-pane>

        <!-- 量价类策略 -->
        <n-tab-pane name="volume" tab="📉 量价类 (2个)">
          <n-collapse>
            <n-collapse-item v-for="s in volumeStrategies" :key="s.id" :title="s.name" :name="s.id">
              <n-descriptions :column="1" bordered>
                <n-descriptions-item label="策略ID">
                  <n-tag>{{ s.id }}</n-tag>
                </n-descriptions-item>
                <n-descriptions-item label="原理">
                  {{ s.principle }}
                </n-descriptions-item>
                <n-descriptions-item label="买入信号">
                  <n-text type="success">{{ s.buySignal }}</n-text>
                </n-descriptions-item>
                <n-descriptions-item label="卖出信号">
                  <n-text type="error">{{ s.sellSignal }}</n-text>
                </n-descriptions-item>
                <n-descriptions-item label="适用场景">
                  {{ s.scene }}
                </n-descriptions-item>
                <n-descriptions-item label="特点">
                  {{ s.feature }}
                </n-descriptions-item>
              </n-descriptions>
            </n-collapse-item>
          </n-collapse>
        </n-tab-pane>

        <!-- 动量类策略 -->
        <n-tab-pane name="momentum" tab="⚡ 动量类 (2个)">
          <n-collapse>
            <n-collapse-item v-for="s in momentumStrategies" :key="s.id" :title="s.name" :name="s.id">
              <n-descriptions :column="1" bordered>
                <n-descriptions-item label="策略ID">
                  <n-tag>{{ s.id }}</n-tag>
                </n-descriptions-item>
                <n-descriptions-item label="原理">
                  {{ s.principle }}
                </n-descriptions-item>
                <n-descriptions-item label="买入信号">
                  <n-text type="success">{{ s.buySignal }}</n-text>
                </n-descriptions-item>
                <n-descriptions-item label="卖出信号">
                  <n-text type="error">{{ s.sellSignal }}</n-text>
                </n-descriptions-item>
                <n-descriptions-item label="适用场景">
                  {{ s.scene }}
                </n-descriptions-item>
                <n-descriptions-item label="特点">
                  {{ s.feature }}
                </n-descriptions-item>
              </n-descriptions>
            </n-collapse-item>
          </n-collapse>
        </n-tab-pane>

        <!-- 特殊类策略 -->
        <n-tab-pane name="special" tab="🔧 特殊类 (4个)">
          <n-collapse>
            <n-collapse-item v-for="s in specialStrategies" :key="s.id" :title="s.name" :name="s.id">
              <n-descriptions :column="1" bordered>
                <n-descriptions-item label="策略ID">
                  <n-tag>{{ s.id }}</n-tag>
                </n-descriptions-item>
                <n-descriptions-item label="原理">
                  {{ s.principle }}
                </n-descriptions-item>
                <n-descriptions-item label="买入信号">
                  <n-text type="success">{{ s.buySignal }}</n-text>
                </n-descriptions-item>
                <n-descriptions-item label="卖出信号">
                  <n-text type="error">{{ s.sellSignal }}</n-text>
                </n-descriptions-item>
                <n-descriptions-item label="适用场景">
                  {{ s.scene }}
                </n-descriptions-item>
                <n-descriptions-item label="特点">
                  {{ s.feature }}
                </n-descriptions-item>
              </n-descriptions>
            </n-collapse-item>
          </n-collapse>
        </n-tab-pane>

        <!-- 使用建议 -->
        <n-tab-pane name="tips" tab="💡 使用建议">
          <n-space vertical>
            <n-card size="small" title="策略组合建议">
              <n-list>
                <n-list-item>
                  <strong>趋势+震荡组合</strong>：先用ADX判断是否有趋势，有趋势用趋势策略，无趋势用震荡策略
                </n-list-item>
                <n-list-item>
                  <strong>价格+成交量组合</strong>：如BOLL+OBV，价格突破需要成交量确认
                </n-list-item>
                <n-list-item>
                  <strong>多策略验证</strong>：多个策略同时发出信号，可信度更高
                </n-list-item>
              </n-list>
            </n-card>

            <n-card size="small" title="参数调整建议">
              <n-list>
                <n-list-item>短线交易：缩短周期参数（如MA用5/10代替5/20）</n-list-item>
                <n-list-item>长线投资：放长周期参数（如MA用10/60代替5/20）</n-list-item>
                <n-list-item>高波动市场：增大阈值（如RSI用25/75代替20/80）</n-list-item>
              </n-list>
            </n-card>

            <n-alert type="warning" title="⚠️ 重要提示">
              <n-list>
                <n-list-item>没有完美的策略，回测结果仅供参考</n-list-item>
                <n-list-item>策略需要结合市场环境和个股特性</n-list-item>
                <n-list-item>建议先用历史数据回测验证效果</n-list-item>
                <n-list-item><strong>投资有风险，入市需谨慎</strong></n-list-item>
              </n-list>
            </n-alert>
          </n-space>
        </n-tab-pane>
      </n-tabs>
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'

// 趋势类策略
const trendStrategies = ref([
  { id: 'ma_cross', name: '均线金叉', principle: '短期均线上穿长期均线，表示趋势转强', buySignal: '短期均线从下往上穿越长期均线', sellSignal: '短期均线从上往下穿越长期均线', scene: '趋势明显的行情，震荡市容易假信号', feature: '参数：短期5天，长期20天' },
  { id: 'macd', name: 'MACD策略', principle: '利用快慢两条指数平滑线的交叉判断趋势', buySignal: 'MACD线上穿信号线（金叉）', sellSignal: 'MACD线下穿信号线（死叉）', scene: '中长线趋势交易', feature: '滞后性较强，但信号可靠' },
  { id: 'boll', name: '布林带策略', principle: '价格在统计区间内波动，突破边界视为异常', buySignal: '价格跌破下轨后反弹', sellSignal: '价格突破上轨后回落', scene: '震荡行情，波段交易', feature: '参数：周期20天，标准差倍数2' },
  { id: 'dmi', name: 'DMI策略', principle: '判断趋势方向和强度', buySignal: '+DI上穿-DI，且ADX大于25', sellSignal: '-DI上穿+DI，且ADX大于25', scene: '趋势行情判断', feature: 'ADX衡量趋势强度，>25表示有趋势' },
  { id: 'atr', name: 'ATR策略', principle: '真实波幅，衡量价格波动性', buySignal: '价格突破下轨（均价-ATR倍数）', sellSignal: '价格突破上轨（均价+ATR倍数）', scene: '波动较大的行情', feature: '自适应波动率' },
  { id: 'sar', name: 'SAR抛物线', principle: '价格与抛物线的位置关系判断趋势转向', buySignal: '价格从下往上穿越SAR线', sellSignal: '价格从上往下穿越SAR线', scene: '趋势行情，止损设在建仓点', feature: '追随趋势，适合设置止损' },
  { id: 'aroon', name: 'Aroon阿隆', principle: '衡量趋势的强度和新高/新低的时机', buySignal: 'Aroon-Up上穿Aroon-Down', sellSignal: 'Aroon-Down上穿Aroon-Up', scene: '趋势识别', feature: '数值0-100，>70表示强趋势' },
  { id: 'vwap', name: 'VWAP策略', principle: '成交量加权平均价，机构常用基准', buySignal: '价格低于VWAP一定比例', sellSignal: '价格高于VWAP一定比例', scene: '日内交易，机构交易基准', feature: '考虑成交量，更有参考价值' },
  { id: 'donchian', name: '唐奇安通道', principle: '价格在一定周期内的最高价和最低价构成的通道', buySignal: '价格突破上轨', sellSignal: '价格跌破下轨', scene: '趋势突破交易', feature: '海龟交易法则的核心指标' },
  { id: 'keltner', name: '肯特纳通道', principle: '类似布林带，但用ATR作为波动单位', buySignal: '价格突破上轨', sellSignal: '价格跌破下轨', scene: '趋势突破，比布林带更稳定', feature: '结合EMA和ATR' },
  { id: 'ichimoku', name: '一目均衡表', principle: '综合多个指标判断趋势、支撑压力', buySignal: '价格在转换线和基准线上方', sellSignal: '价格在转换线和基准线下方', scene: '综合趋势判断', feature: '信息量大，一图看清趋势' },
  { id: 'dema', name: 'DEMA策略', principle: '双指数移动平均，减少滞后', buySignal: '短期DEMA上穿长期DEMA', sellSignal: '短期DEMA下穿长期DEMA', scene: '趋势交易', feature: '比普通MA更快响应' },
  { id: 'expma', name: 'EXPMA策略', principle: '指数平滑移动平均，近期权重更大', buySignal: '短期EXPMA上穿长期EXPMA', sellSignal: '短期EXPMA下穿长期EXPMA', scene: '中短线趋势交易', feature: '反应灵敏' },
])

// 震荡类策略
const oscillatorStrategies = ref([
  { id: 'kdj', name: 'KDJ策略', principle: '随机指标，判断超买超卖', buySignal: 'J值低于20（超卖区）', sellSignal: 'J值高于80（超买区）', scene: '震荡行情', feature: '趋势行情容易钝化' },
  { id: 'rsi', name: 'RSI策略', principle: '相对强弱指数，衡量买卖力量', buySignal: 'RSI低于30（超卖）', sellSignal: 'RSI高于70（超买）', scene: '震荡行情', feature: '数值0-100' },
  { id: 'cci', name: 'CCI策略', principle: '顺势指标，衡量价格偏离程度', buySignal: 'CCI低于-100（超卖）', sellSignal: 'CCI高于+100（超买）', scene: '震荡行情', feature: '比RSI更敏感' },
  { id: 'wr', name: 'WR威廉指标', principle: '衡量超买超卖程度', buySignal: 'WR低于-80（超卖）', sellSignal: 'WR高于-20（超买）', scene: '震荡行情', feature: 'WR数值是负数' },
  { id: 'bias', name: 'BIAS乖离率', principle: '价格偏离均线的程度', buySignal: '乖离率低于-10%（超卖）', sellSignal: '乖离率高于+10%（超买）', scene: '震荡行情，价格回归', feature: '均值回归思想' },
  { id: 'stochastic', name: 'Stochastic随机指标', principle: '收盘价在周期内的相对位置', buySignal: 'K线上穿D线，且在超卖区', sellSignal: 'K线下穿D线，且在超买区', scene: '震荡行情', feature: '比KDJ更稳定' },
  { id: 'mfi', name: 'MFI资金流量', principle: '结合价格和成交量的RSI', buySignal: 'MFI低于20（超卖）', sellSignal: 'MFI高于80（超买）', scene: '量价结合分析', feature: '考虑成交量，更真实' },
])

// 量价类策略
const volumeStrategies = ref([
  { id: 'volume_price', name: '量价策略', principle: '放量上涨/下跌的信号意义', buySignal: '放量大涨（成交量>1.5倍均量，涨幅>3%）', sellSignal: '放量大跌', scene: '捕捉资金动向', feature: '关注成交量变化' },
  { id: 'obv', name: 'OBV能量潮', principle: '累积成交量变化判断资金流向', buySignal: 'OBV上穿均线', sellSignal: 'OBV下穿均线', scene: '趋势确认', feature: '提前发现趋势变化' },
])

// 动量类策略
const momentumStrategies = ref([
  { id: 'mom', name: 'MOM动量指标', principle: '当前价格与N天前价格的差值', buySignal: '动量由负转正', sellSignal: '动量由正转负', scene: '趋势转折判断', feature: '简单直接' },
  { id: 'roc', name: 'ROC变动率', principle: '价格变动的百分比', buySignal: 'ROC低于-10%（超卖）', sellSignal: 'ROC高于+10%（超买）', scene: '震荡行情', feature: '比MOM更直观' },
])

// 特殊类策略
const specialStrategies = ref([
  { id: 'trix', name: 'TRIX三重指数', principle: '三重平滑过滤短期波动', buySignal: 'TRIX上穿信号线', sellSignal: 'TRIX下穿信号线', scene: '趋势行情，过滤噪音', feature: '非常平滑' },
  { id: 'adx', name: 'ADX平均趋向', principle: '衡量趋势强度，不判断方向', buySignal: 'ADX>25表示有趋势', sellSignal: 'ADX<20表示无趋势', scene: '判断是否适合用趋势策略', feature: 'ADX只表示强度，不表示方向' },
  { id: 'bbi', name: 'BBI多空指标', principle: '多条均线的平均值', buySignal: '价格上穿BBI线', sellSignal: '价格下穿BBI线', scene: '综合多条均线判断', feature: '减少单一均线假信号' },
  { id: 'zigzag', name: 'ZigZag之字形', principle: '过滤小波动，显示主要转折', buySignal: '趋势转折向上', sellSignal: '趋势转折向下', scene: '识别主要趋势', feature: '过滤噪音，滞后性强' },
])
</script>

<style scoped>
.strategy-guide {
  max-width: 1200px;
  margin: 0 auto;
}
</style>