// Package config 定义调度器可配置项。
package config

import "time"

// Config 控制调度周期、重试与退避策略。
type Config struct {
	ScheduleInterval  time.Duration // 调度周期
	MaxBindRetries    int           // 绑定冲突时的最大重试次数
	RetryBackoffBase  time.Duration // 退避基数，第 n 次重试等待 base << n
	RetryBackoffMax   time.Duration // 退避上限
	PreemptionEnabled bool          // 是否允许高优抢占低优
}

// Default 返回默认配置。
func Default() Config {
	return Config{
		ScheduleInterval:  100 * time.Millisecond,
		MaxBindRetries:    5,
		RetryBackoffBase:  time.Millisecond,
		RetryBackoffMax:   100 * time.Millisecond,
		PreemptionEnabled: true,
	}
}

// Backoff 计算第 attempt（从 0 起）次重试的退避时长，指数增长并封顶。
func (c Config) Backoff(attempt int) time.Duration {
	d := c.RetryBackoffBase
	for i := 0; i < attempt; i++ {
		d *= 2
		if d >= c.RetryBackoffMax {
			return c.RetryBackoffMax
		}
	}
	if d > c.RetryBackoffMax {
		return c.RetryBackoffMax
	}
	return d
}
