package main

import "regexp"

/*
正常系
	10,897,603
異常系
	※1 10,897,603
	※1,※2 10,897,603
*/
// 数字のみのパターン (※１ などを除外するためのパターン)
// 数字の前に※がないことを条件に加える
var OnlyNumRe *regexp.Regexp = regexp.MustCompile(`\d+`)

// var OnlyNumRe *regexp.Regexp = regexp.MustCompile(`(?!※)\d+`)

// ※1 などを除外するためのパターン
var AsteriskAndHalfWidthNumRe *regexp.Regexp = regexp.MustCompile(`※\d+`)
