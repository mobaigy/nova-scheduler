package interfaces

import (
	"context"
	"fmt"
	"github.com/BitofferHub/pkg/constant"
	"github.com/BitofferHub/pkg/middlewares/log"
	pb "github.com/BitofferHub/proto_center/api/xtimer/v1"
	"github.com/BitofferHub/xtimer/internal/response"
	"github.com/gin-gonic/gin"
	"strconv"
)

// CreateTimer 创建定时器
func (h *Handler) CreateTimer(c *gin.Context) {
	traceID := c.Request.Header.Get(constant.TraceID)

	var req pb.CreateTimerRequest

	// ShouldBind是从请求体中解析对象
	if err := c.ShouldBind(&req); err != nil {
		log.Errorf("CreateTimer err: %v", err)
		response.Fail(c, response.ParamError, nil)
		return
	}

	ctx := context.WithValue(context.Background(), constant.TraceID, traceID)
	resp, err := h.xTimerService.CreateTimer(ctx, &req)
	if err != nil {
		fmt.Println("Create timer err: %v", err)
		response.Fail(c, response.ParamError, nil)
		return
	}

	response.Success(c, resp)
}

// EnableTimer 激活定时器
func (h *Handler) EnableTimer(c *gin.Context) {
	traceID := c.Request.Header.Get(constant.TraceID)
	// timerId需要进行类型转换，必须手动赋值
	timerId, err := strconv.ParseInt(c.Query("timerId"), 10, 64)
	if err != nil {
		log.Errorf("EnableTimer err: %v", err)
		response.Fail(c, response.ParamError, nil)
		return
	}
	req := pb.EnableTimerRequest{
		TimerId: timerId,
		App:     c.Query("app"),
	}

	// ShouldBindQuery是从URL查询参数中解析对象
	if err := c.ShouldBindQuery(&req); err != nil {
		log.Errorf("EnableTimer err: %v", err)
		response.Fail(c, response.ParamError, nil)
		return
	}

	ctx := context.WithValue(context.Background(), constant.TraceID, traceID)
	_, err = h.xTimerService.EnableTimer(ctx, &req)
	if err != nil {
		log.Errorf("EnableTimer err: %v", err)
		response.Fail(c, response.EnableTimerError, nil)
		return

	}
	response.Success(c, nil)
	return
}

// TestCallback 测试回调
func (h *Handler) TestCallback(c *gin.Context) {
	log.Info("callback test: %v", c.Request.Body)

	response.Success(c, "ok: callback receives")
}
