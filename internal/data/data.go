package data

import (
	"context"
	"github.com/BitofferHub/pkg/middlewares/cache"
	"github.com/BitofferHub/pkg/middlewares/gormcli"
	"github.com/BitofferHub/xtimer/internal/biz"
	"github.com/BitofferHub/xtimer/internal/conf"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/wire"
	"gorm.io/gorm"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(
	NewData, NewXtimerRepo, NewDatabase, NewCache, NewTransaction, biz.NewHttpClient,
)

type contextTxKey struct{}

// 开启一个事务，把这个事务对象添加到ctx 中，让后续调用 DB 的代码自动使用同一个事务执行
func (d *Data) InTx(ctx context.Context, fn func(ctx context.Context) error) error {
	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 把事务 tx 存入 context
		ctx = context.WithValue(ctx, contextTxKey{}, tx)
		return fn(ctx)
	})
}

func (d *Data) DB(ctx context.Context) *gorm.DB {
	tx, ok := ctx.Value(contextTxKey{}).(*gorm.DB)
	if ok {
		// 返回一个事务对象
		return tx
	}
	return d.db
}

func NewTransaction(d *Data) biz.Transaction {
	return d
}

type Data struct {
	db    *gorm.DB
	cache *cache.Client
}

func NewData(db *gorm.DB, cache *cache.Client) *Data {
	dt := &Data{db: db, cache: cache}
	return dt
}

func NewDatabase(conf *conf.Data) *gorm.DB {
	dt := conf.GetDatabase()
	gormcli.Init(
		gormcli.WithAddr(dt.GetAddr()),
		gormcli.WithUser(dt.GetUser()),
		gormcli.WithPassword(dt.GetPassword()),
		gormcli.WithDataBase(dt.GetDatabase()),
		gormcli.WithMaxIdleConn(int(dt.GetMaxIdleConn())),
		gormcli.WithMaxOpenConn(int(dt.GetMaxOpenConn())),
		gormcli.WithMaxIdleTime(int64(dt.GetMaxIdleTime())),
		// 如果设置了慢日志阈值, 则打印日志，否则不打印
		gormcli.WithSlowThresholdMillisecond(dt.GetSlowThresholdMillisecond()))

	return gormcli.GetDB()
}

func NewCache(conf *conf.Data) *cache.Client {
	dt := conf.GetRedis()
	cache.Init(
		cache.WithAddr(dt.GetAddr()),
		cache.WithPassWord(dt.GetPassword()),
		cache.WithDB(int(dt.GetDb())),
		cache.WithPoolSize(int(dt.GetPoolSize())))

	return cache.GetRedisCli()
}
