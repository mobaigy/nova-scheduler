package biz

import (
	"context"
	"fmt"
	"github.com/BitofferHub/pkg/middlewares/log"
	"github.com/BitofferHub/xtimer/internal/conf"
	"github.com/BitofferHub/xtimer/internal/constant"
	"github.com/BitofferHub/xtimer/internal/utils"
	"strconv"
	"strings"
	"sync"
	"time"
)

// xtimerUseCase is a User usecase.
type TriggerUseCase struct {
	confData  *conf.Data
	timerRepo TimerRepo
	taskRepo  TimerTaskRepo
	taskCache TaskCache
	tm        Transaction
	pool      WorkerPool
	executor  *ExecutorUseCase
}

// NewUserUseCase new a User usecase.
func NewTriggerUseCase(confData *conf.Data, timerRepo TimerRepo, taskRepo TimerTaskRepo, taskCache TaskCache, executorUseCase *ExecutorUseCase) *TriggerUseCase {
	return &TriggerUseCase{
		confData:  confData,
		timerRepo: timerRepo,
		taskRepo:  taskRepo,
		taskCache: taskCache,
		pool:      NewGoWorkerPool(int(confData.Trigger.WorkersNum)),
		executor:  executorUseCase,
	}
}

func (w *TriggerUseCase) Work(ctx context.Context, minuteBucketKey string, ack func()) error {

	// 解析出起始时间
	startTime, err := getStartMinute(minuteBucketKey)
	if err != nil {
		return err
	}

	// 定义 ticker，用于每隔 ZrangeGapSeconds（1s） 执行一次
	ticker := time.NewTicker(time.Duration(w.confData.Trigger.ZrangeGapSeconds) * time.Second)
	defer ticker.Stop()

	// 传递错误
	notifier := NewSafeChan(int(time.Minute / (time.Duration(w.confData.Trigger.ZrangeGapSeconds) * time.Second)))
	defer notifier.Close()

	// endTime = startTime + 1min
	endTime := startTime.Add(time.Minute)
	var wg sync.WaitGroup
	for range ticker.C {
		select {
		case e := <-notifier.GetChan():
			err, _ = e.(error)
			return err
		default:
		}

		// 扫描任务
		wg.Add(1)
		go func(startTime time.Time) {
			defer wg.Done()
			if err := w.handleBatch(ctx, minuteBucketKey, startTime, startTime.Add(time.Duration(w.confData.Trigger.ZrangeGapSeconds)*time.Second)); err != nil {
				notifier.Put(err)
			}
		}(startTime)

		// 扫描了1min后，跳出循环
		if startTime = startTime.Add(time.Duration(w.confData.Trigger.ZrangeGapSeconds) * time.Second); startTime.Equal(endTime) || startTime.After(endTime) {
			break
		}
	}

	wg.Wait()
	select {
	case e := <-notifier.GetChan():
		err, _ = e.(error)
		return err
	default:
	}
	// 延长锁的过期时间
	ack()
	log.InfoContextf(ctx, "ack success, key: %s", minuteBucketKey)
	return nil
}

// 将当前时间段内应触发的任务取出并执行
func (w *TriggerUseCase) handleBatch(ctx context.Context, key string, start, end time.Time) error {
	// 获取当前桶编号
	bucket, err := getBucket(key)
	if err != nil {
		return err
	}

	// 获取该时间范围+该桶内的任务
	tasks, err := w.getTasksByTime(ctx, key, bucket, start, end)
	if err != nil || len(tasks) == 0 {
		return err
	}

	// 获取这些任务的定时器ID
	timerIDs := make([]int64, 0, len(tasks))
	for _, task := range tasks {
		timerIDs = append(timerIDs, task.TimerID)
	}

	for _, task := range tasks {
		task := task
		// 向协程池提交任务
		if err := w.pool.Submit(func() {
			//异步执行
			if err := w.executor.Work(ctx, utils.UnionTimerIDUnix(uint(task.TimerID), task.RunTimer)); err != nil {
				log.ErrorContextf(ctx, "executor work failed, err: %v", err)
			}
		}); err != nil {
			return err
		}
	}
	return nil
}

// 获取这段时间应该执行的任务
func (w *TriggerUseCase) getTasksByTime(ctx context.Context, key string, bucket int, start, end time.Time) ([]*TimerTask, error) {
	// 先走缓存
	tasks, err := w.taskCache.GetTasksByTime(ctx, key, start.UnixMilli(), end.UnixMilli())
	if err == nil {
		return tasks, nil
	}

	// 倘若缓存查询报错，再走db
	tasks, err = w.taskRepo.GetTasksByTimeRange(ctx, start.UnixMilli(), end.UnixMilli(), constant.NotRunned.ToInt())
	if err != nil {
		return nil, err
	}

	// 获取系统配置的 bucket 总数
	maxBucket := w.confData.Scheduler.BucketsNum
	var validTask []*TimerTask
	for _, task := range tasks {
		// 根据 TimerID%BucketNum 规则过滤任务
		if uint(task.TimerID)%uint(maxBucket) != uint(bucket) {
			continue
		}
		validTask = append(validTask, task)
	}

	return validTask, nil
}

// 获取执行时间
func getStartMinute(slice string) (time.Time, error) {
	timeBucket := strings.Split(slice, "_")
	if len(timeBucket) != 2 {
		return time.Time{}, fmt.Errorf("invalid format of msg key: %s", slice)
	}

	return utils.GetStartMinute(timeBucket[0])
}

// 获取桶编号
func getBucket(slice string) (int, error) {
	timeBucket := strings.Split(slice, "_")
	if len(timeBucket) != 2 {
		return -1, fmt.Errorf("invalid format of msg key: %s", slice)
	}
	return strconv.Atoi(timeBucket[1])
}
