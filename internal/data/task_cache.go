package data

import (
	"context"
	"fmt"
	"github.com/BitofferHub/xtimer/internal/biz"
	"github.com/BitofferHub/xtimer/internal/conf"
	"github.com/BitofferHub/xtimer/internal/constant"
	"github.com/BitofferHub/xtimer/internal/utils"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

type TaskCache struct {
	confData *conf.Data
	data     *Data
}

func NewTaskCache(confData *conf.Data, data *Data) *TaskCache {
	return &TaskCache{confData: confData, data: data}
}

// 写入批量定时任务到Zset中，任务执行时间戳为 score，任务ID+时间戳为 member
func (t *TaskCache) BatchCreateTasks(ctx context.Context, tasks []*biz.TimerTask) error {
	if len(tasks) == 0 {
		return nil
	}

	err := t.data.cache.Pipeline(ctx, func(pipe redis.Pipeliner) error {

		for _, task := range tasks {
			// 获取时间戳
			unix := task.RunTimer
			// 获取任务对应Zset的key名字
			tableName := t.GetTableName(task)
			var members []redis.Z
			// 获取score member
			member := redis.Z{Score: float64(unix), Member: utils.UnionTimerIDUnix(uint(task.TimerID), unix)}
			members = append(members, member)
			// 存储到redis中
			pipe.ZAdd(ctx, tableName, members...)

			// zset 一天后过期
			aliveDuration := time.Until(time.UnixMilli(task.RunTimer).Add(24 * time.Hour))
			pipe.Expire(ctx, tableName, aliveDuration)
		}
		return nil
	})
	return err
}

func (t *TaskCache) GetTasksByTime(ctx context.Context, table string, start, end int64) ([]*biz.TimerTask, error) {
	timerIDUnixs, err := t.data.cache.ZRangeByScore(ctx, table, strconv.FormatInt(start, 10), strconv.FormatInt(end-1, 10))
	if err != nil {
		return nil, err
	}

	tasks := make([]*biz.TimerTask, 0, len(timerIDUnixs))
	for _, timerIDUnix := range timerIDUnixs {
		timerID, unix, _ := utils.SplitTimerIDUnix(timerIDUnix)
		tasks = append(tasks, &biz.TimerTask{
			TimerID:  int64(timerID),
			RunTimer: unix,
		})
	}

	return tasks, nil
}

// 计算Zset的key名，由时间+桶编号组成唯一key
func (t *TaskCache) GetTableName(task *biz.TimerTask) string {
	maxBucket := t.confData.Scheduler.BucketsNum
	return fmt.Sprintf("%s_%d", time.UnixMilli(task.RunTimer).Format(constant.MinuteFormat), int64(task.TimerID)%int64(maxBucket))
}
