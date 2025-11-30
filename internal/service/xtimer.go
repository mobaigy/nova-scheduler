package service

import (
	"context"
	"encoding/json"
	pb "github.com/BitofferHub/proto_center/api/xtimer/v1"
	"github.com/BitofferHub/xtimer/internal/biz"
	"github.com/BitofferHub/xtimer/internal/constant"
)

func (s *XTimerService) CreateTimer(ctx context.Context, req *pb.CreateTimerRequest) (*pb.CreateTimerReply, error) {
	// 将HTTP请求通知参数序列化为JSON格式
	param, err := json.Marshal(req.NotifyHTTPParam)
	if err != nil {
		return nil, err
	}
	// 创建定时器
	timer, err := s.timerUC.CreateTimer(ctx, &biz.Timer{
		App:             req.App,
		Name:            req.Name,
		Status:          constant.Unabled.ToInt(),
		Cron:            req.Cron,
		NotifyHTTPParam: string(param),
	})
	if err != nil {
		return nil, err
	}
	// 创建成功，返回一个grpc响应
	return &pb.CreateTimerReply{Code: 0, Message: "ok", Data: &pb.CreateTimerReplyData{
		TimerId: timer.TimerId,
	}}, nil
}
func (s *XTimerService) EnableTimer(ctx context.Context, req *pb.EnableTimerRequest) (*pb.EnableTimerReply, error) {
	// 激活定时器
	err := s.timerUC.EnableTimer(ctx, req.GetApp(), req.GetTimerId())
	if err != nil {
		return nil, err
	}
	// 激活成功，返回一个grpc响应
	return &pb.EnableTimerReply{Code: 0, Message: "ok"}, nil
}
