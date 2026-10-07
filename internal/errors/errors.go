// Package errors 定义摄取框架的统一错误分类。
// 所有对外接口返回的错误都必须归类为 *Error，不得透出底层库原始错误。
package errors

import (
	"errors"
	"fmt"
)

// Class 是错误分类标识，用于日志与测试断言。
type Class string

const (
	ClassRegistrationConflict   Class = "registration_conflict"    // 连接器注册标识冲突
	ClassDuplicateInit          Class = "duplicate_init"           // 数据源重复初始化
	ClassConfigInvalid          Class = "config_invalid"           // 数据源配置写错或缺少必要配置
	ClassUnknownConnectorKind   Class = "unknown_connector_kind"   // 指定的数据源类型不存在
	ClassRecordInvalid          Class = "record_invalid"           // 记录内容不合规（必填缺失/字段形态不对）
	ClassRecordUnparseable      Class = "record_unparseable"       // 整行/整文件无法按格式解析
	ClassPositionRegression     Class = "position_regression"      // 位点回退
	ClassPositionOutOfRange     Class = "position_out_of_range"    // 位点越界
	ClassPositionSourceMismatch Class = "position_source_mismatch" // 位点与数据源不匹配
	ClassSourceExhausted        Class = "source_exhausted"         // 数据源读取完毕
	ClassSinkFailure            Class = "sink_failure"             // 下游写入失败
	ClassBackpressureTimeout    Class = "backpressure_timeout"     // 背压等待超时
	ClassCanceled               Class = "canceled"                 // 上下文取消
	ClassInternal               Class = "internal"                 // 未分类内部错误
)

// IsRecordLevel 报告 err 是否为记录级错误（坏记录）。
// 记录级错误可跳过续跑；其余错误均为致命错误。
func IsRecordLevel(err error) bool {
	switch ClassOf(err) {
	case ClassRecordInvalid, ClassRecordUnparseable:
		return true
	}
	return false
}

// Error 是框架对外暴露的唯一错误类型。
type Error struct {
	Class Class
	Op    string // 出错的操作名
	Msg   string
	Err   error // 被包装的底层错误，仅用于 Unwrap，不直接透出
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %s: %v", e.Class, e.Op, e.Msg, e.Err)
	}
	return fmt.Sprintf("%s: %s: %s", e.Class, e.Op, e.Msg)
}

func (e *Error) Unwrap() error { return e.Err }

// New 构造一个无底层原因的错误。
func New(class Class, op, msg string) *Error {
	return &Error{Class: class, Op: op, Msg: msg}
}

// Newf 构造一个带格式化消息的错误。
func Newf(class Class, op, format string, args ...any) *Error {
	return &Error{Class: class, Op: op, Msg: fmt.Sprintf(format, args...)}
}

// Wrap 用底层错误构造分类错误。
func Wrap(class Class, op string, err error) *Error {
	return &Error{Class: class, Op: op, Msg: err.Error(), Err: err}
}

// ClassOf 提取错误的分类；非框架错误归为 ClassInternal。
func ClassOf(err error) Class {
	var e *Error
	if errors.As(err, &e) {
		return e.Class
	}
	return ClassInternal
}

// Is 报告 err 是否为指定分类的框架错误。
func Is(err error, class Class) bool { return ClassOf(err) == class }
