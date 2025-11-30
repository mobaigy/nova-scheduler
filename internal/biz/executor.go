package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/BitofferHub/pkg/middlewares/log"
	v1 "github.com/BitofferHub/proto_center/api/xtimer/v1"
	"github.com/BitofferHub/xtimer/internal/conf"
	"github.com/BitofferHub/xtimer/internal/constant"
	"github.com/BitofferHub/xtimer/internal/utils"
	"time"
)

// xtimerUseCase is a User usecase.
type ExecutorUseCase struct {
	confData   *conf.Data
	httpClient *JSONClient
	timerRepo  TimerRepo
	taskRepo   TimerTaskRepo
}

// NewUserUseCase new a User usecase.
func NewExecutorUseCase(confData *conf.Data, timerRepo TimerRepo, taskRepo TimerTaskRepo, taskCache TaskCache, httpClient *JSONClient) *ExecutorUseCase {
	return &ExecutorUseCase{
		confData:   confData,
		timerRepo:  timerRepo,
		taskRepo:   taskRepo,
		httpClient: httpClient,
	}
}

func (w *ExecutorUseCase) Work(ctx context.Context, timerIDUnixKey string) error {
	// 拿到消息，查询一次完整的 timer 定义
	timerID, unix, err := utils.SplitTimerIDUnix(timerIDUnixKey)
	if err != nil {
		return err
	}
	return w.executeAndPostProcess(ctx, timerID, unix)
}

func (w *ExecutorUseCase) executeAndPostProcess(ctx context.Context, timerID int64, unix int64) error {
	// 查询数据库，拿到定时器
	timer, err := w.timerRepo.FindByID(ctx, timerID)
	if err != nil {
		return fmt.Errorf("get timer failed, id: %d, err: %w", timerID, err)
	}

	// 若定时器已经处于去激活态，则无需处理任务
	if timer.Status != constant.Enabled.ToInt() {
		log.WarnContextf(ctx, "timer has alread been unabled, timerID: %d", timerID)
		return nil
	}

	// 执行定时器的回调，通知业务方
	execTime := time.Now()
	resp, err := w.execute(ctx, timer)

	// 执行后置处理
	return w.postProcess(ctx, resp, err, timer.App, uint(timerID), unix, execTime)
}

// 执行一次定时器的回调
func (w *ExecutorUseCase) execute(ctx context.Context, timer *Timer) (map[string]interface{}, error) {
	var (
		resp map[string]interface{}
		err  error
	)

	notifyHTTPParam := v1.NotifyHTTPParam{}
	// 将JSON数据反序列化成结构体
	err = json.Unmarshal([]byte(timer.NotifyHTTPParam), &notifyHTTPParam)
	if err != nil {
		log.Errorf("json unmarshal for NotifyHTTPParam err %s", err.Error())
		return nil, err
	}

	// 发起POST请求，通知业务方处理数据
	err = w.httpClient.Post(ctx, notifyHTTPParam.Url, notifyHTTPParam.Header, notifyHTTPParam.Body, &resp)
	return resp, err
}

// 执行后处理
func (w *ExecutorUseCase) postProcess(ctx context.Context, resp map[string]interface{}, execErr error, app string, timerID uint, unix int64, execTime time.Time) error {
	// 在数据库中查找这一条任务
	task, err := w.taskRepo.GetTasksByTimerIdAndRunTimer(ctx, int64(timerID), unix)
	if err != nil {
		return fmt.Errorf("get task failed, timerID: %d, runTimer: %d, err: %w", timerID, time.UnixMilli(unix), err)
	}

	// 保存回调执行结果
	if execErr != nil {
		task.Output = execErr.Error()
	} else {
		respBody, _ := json.Marshal(resp)
		task.Output = string(respBody)
	}

	// 更新任务状态
	if execErr != nil {
		task.Status = constant.Failed.ToInt()
	} else {
		task.Status = constant.Successed.ToInt()
	}

	// 计算任务执行耗时
	task.CostTime = int(execTime.UnixMilli() - task.RunTimer)

	// 将上述的变化再写入到数据库中
	_, err = w.taskRepo.Update(ctx, task)
	if err != nil {
		return fmt.Errorf("task postProcess failed, timerID: %d, runTimer: %d, err: %w", timerID, time.UnixMilli(unix), err)
	}

	return nil
}
