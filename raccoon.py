# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date     : 2019/6/10
# @Author   : Deyong ZHAN
# @Version  : v3.0.0

import datetime as dt
import numpy as np
import os
import pandas as pd
import random
import re
import time
import traceback
import warnings


warnings.filterwarnings("ignore")

def check_blank(df, columns, primary=None):
    """
    检查数据表指定列的取值中是否存在空格
    :param df: DataFrame, 待检查的数据表名称
    :param columns: list, 需要执行空格检查的列名
    :param primary: list, 具有唯一标识性的列名, 输出时用来区分各行的“主键”, 默认为空
    :return: None
    """
    df_bool = df[columns].applymap(lambda x: True if re.search('\s', str(x)) else False)
    if df_bool.sum().sum() > 0:
        print("\n下列数据取值存在空白字符")
        if primary:
            print(df[primary + columns][df_bool.sum(1) > 0])
        else:
            print(df[columns][df_bool.sum(1) > 0])
    else:
        print("\n检查完毕，没有发现空格")


def drop_blank(df, columns, inplace=False):
    """
    剔除数据表指定列的取值中的空格，注意有多个空格时需要多次执行
    :param df: DataFrame
    :param columns: list, 需要删除空格的列名
    :param inplace: boolean, 是否替换原来的数据表, 默认不替换
    :return: DataFrame
    """
    if inplace:
        df[columns] = df[columns].applymap(lambda x: str(x).replace(" ", ""))
    else:
        df2 = df.copy()
        df2[columns] = df2[columns].applymap(lambda x: str(x).replace(" ", ""))
        return df2


def check_upper_letter(df, columns, primary=None):
    """
    检查数据表指定列的取值中是否存在大写字母
    :param df: DataFrame, 待检查的数据表名称
    :param columns: list, 需要执行正则表达式检查的列名
    :param primary: list, 具有唯一标识性的列名, 输出时用来区分各行的“主键”, 默认为空
    :return: None
    """
    df_bool = df[columns].applymap(lambda x: True if re.search('[A-Z]', str(x)) else False)
    if df_bool.sum().sum() > 0:
        print("\n下列规则的正则表达式存在大写字母")
        if primary:
            print(df[primary + columns][df_bool.sum(1) > 0])
        else:
            print(df[columns][df_bool.sum(1) > 0])
    else:
        print("\n检查完毕，没有发现大写字母")


def lower_str(df, columns, inplace=False):
    """
    将数据表指定列的取值（字母）全部转成小写
    :param df: DataFrame, 待检查的数据表名称
    :param columns: list, 需要转成小写的列名
    :param inplace: bool, 是否替换原来的数据表, 默认不替换
    :return: DataFrame
    """
    if inplace:
        df[columns] = df[columns].applymap(lambda x: str(x).lower())
    else:
        df2 = df.copy()
        df2[columns] = df2[columns].applymap(lambda x: str(x).lower())
        return df2


def check_or_pattern(df, columns, primary=None, single=False):
    """
    检查数据表指定列的取值中是否存在多余的"|"(“或”正则表达式字符），包括开头、结尾、或重复等情况
    :param df: DataFrame, 待检查的数据表名称
    :param columns: list, 需要执行正则表达式检查的列名
    :param primary: list, 具有唯一标识性的列名, 输出时用来区分各行的“主键”, 默认为空
    :param single, bool, 是否检查取值仅为单个"|"的情况， 默认忽略
    :return: None
    """
    if not primary:
        primary = []
    if single:
        def single_or_pattern_filter(x):
            x = str(x)
            if re.search('^\|.+', x) or re.search('.+\|$', x) or re.search('\|\|', x) or re.search('\|\n\|', x) or \
                    re.search('^\|$', x):
                return True
            else:
                return False
        df_bool = df[primary + columns].applymap(single_or_pattern_filter)
    else:
        def or_pattern_filter(x):
            x = str(x)
            if re.search('^\|.+', x) or re.search('.+\|$', x) or re.search('\|\|', x) or re.search('\|\n\|', x):
                return True
            else:
                return False
        df_bool = df[primary + columns].applymap(or_pattern_filter)
    if df_bool.sum().sum() > 0:
        print("\n下列规则的正则表达式可能存在多余的'|':\n")
        print(df[primary + columns][df_bool.sum(1) > 0])
    else:
        print("\n检查完毕，没有发现多余的'|'\n")


def check_bracket_pattern(df, columns, primary=None):
    """
    检查数据表指定列的取值中是否存在中文括号
    :param df: DataFrame, 待检查的数据表名称
    :param columns: list, 需要执行正则表达式检查的列名
    :param primary: list, 具有唯一标识性的列名, 输出时用来区分各行的“主键”, 默认为空
    :return: None
    """
    if not primary:
        primary = []

    def bracket_pattern_filter(x):
        x = str(x)
        if re.search('）', x) or re.search('（', x):
            return True
        else:
            return False
    df_bool = df[columns].applymap(bracket_pattern_filter)
    if df_bool.sum().sum() > 0:
        print("\n下列规则的正则表达式可能存在中文括号:\n")
        print(df[primary + columns][df_bool.sum(1) > 0])
    else:
        print("\n检查完毕，没有发现中文括号。\n")


def check_escape_pattern(df, columns):
    """
    检查数据表指定列的取值中带有转义字符的正则表达式（结果去重）
    :param df: DataFrame, 待检查的数据表名称
    :param columns: list, 需要执行正则表达式检查的列名
    :return: set
    """
    dic_escape = dict()
    set_escape = set()
    for col in columns:
        s_escape = df[col].astype(str).str.findall(r"\\.{1}")[df[col].astype(str).str.contains(r"\\.{1}")]
        dic_escape[col] = []
        for i in s_escape:
            dic_escape[col] = dic_escape[col] + i
        dic_escape[col] = list(set(dic_escape[col]))
        set_escape = set_escape | set(dic_escape[col])
    print("各字段中含有的转义字符分别为:\n\t%s" % dic_escape)
    print("所有字段含有的转义字符（去重）为:\n\t%s" % set_escape)


def func_where_sql(df, white, black=None, lower=None, upper=None):
    """
    基于各字段的黑白清洗/筛选规则（正则表达式）生成相应的HQL筛选条件
    :param df: 清洗/筛选规则表，采用python表达式语法描述各样本相应字段需要满足的清洗/筛选规则
    :param white: {str: str}, {数据库字段名：规则表相应的白名单字段名}
    :param black: {str: str}, {数据库字段名：规则字表相应的黑名单字段名}
    :param lower: list, 执行SQL时需要先将取值转成小写再执行规则的字段名，默认为空
    :param upper: list, 执行SQL需要先将取值转成大写再执行规则的字段名，默认为空
    :return: str, where部分的HiveSQL语句
    """
    if not lower:
        lower = list()
    if not upper:
        upper = list()
    condition = list()
    if white:
        for col in white.keys():
            if str(df[white[col]]) in ['|', "nan", 'None', ""]:
                continue
            elif col in lower and len(lower) > 0:
                condition.append("lower(" + col + ") rlike \"" + df[white[col]] + "\"")
            elif col in upper and len(upper) > 0:
                condition.append("upper(" + col + ") rlike \"" + df[white[col]] + "\"")
            else:
                condition.append(col + " rlike \"" + str(df[white[col]]) + "\"")
    if black:
        for col in black.keys():
            if str(df[black[col]]) in ['/', "nan", 'None', ""]:
                continue
            elif col in lower and len(lower) > 0:
                condition.append("lower(" + col + ") not rlike \"" + df[black[col]] + "\"")
            elif col in lower and len(upper) > 0:
                condition.append("upper(" + col + ") not rlike \"" + df[black[col]] + "\"")
            else:
                condition.append(col + " not rlike \"" + str(df[black[col]]) + "\"")
    if condition:
        return "(" + " and ".join(condition) + ")"
    else:
        return ""


def sql_where_expression(df, white, black=None, lower=None, upper=None):
    """
    基于各字段的黑白规则生成相应的HQL筛选条件(where语句)
    :param df: DataFrame, 含有各字段白/黑规则（正则表达式）的规则表
    :param white: {str: str}, {数据库字段名：关键字表相应的白名单字段名}
    :param black: {str: str}, {数据库字段名：关键字表相应的黑名单字段名}
    :param lower: list, 执行SQL时需要先将取值转成小写再执行规则的字段名，默认为空
    :param upper: list, 执行SQL需要先将取值转成大写再执行规则的字段名，默认为空
    :return: Series
    """
    if black:
        columns = list(set(list(white.values()) + list(black.values())))
    else:
        columns = list(white.values())
    return df[columns].apply(func_where_sql, axis=1, white=white, black=black, lower=lower, upper=upper)


def join_sql_where_expression(condition, how="or"):
    """

    :param condition: Series, SQL逻辑表达式
    :param how: str, SQL逻辑表达式的连接方法，如'and'或'or'
    :return: str
    """
    sql = "\n\t(\n\t\t" + (" \n\t\t" + how + " ").join(condition) + "\n\t)"
    return sql


def create_sql(select, from_table, head="", create_table=None, location=None, where=None, group_by=None, order_by=None,
               limit=None):
    """
    生成HQL
    :param select: str, select语句中的字段名
    :param from_table: str, from语句中的表名
    :param head: str, 系统设置语句
    :param create_table, str, create语句中的表名
    :param location: str, create语句中的存储位置
    :param where: str, where语句中的表达式
    :param group_by: str, group by语句中的字段名
    :param order_by: str, order by语句中的字段名
    :param limit: int, limit语句中的行数
    :return: str, hql语句
    """
    if create_table:
        drop_sql = "\ndrop table if exists " + create_table + ";"
        if location:
            create = "\ncreate table " + create_table + " stored as orcfile location '" + location + "' as\n"
        else:
            create = "\ncreate table " + create_table + " stored as orcfile as\n"
    else:
        drop_sql, create = "", ""
    select_sql = "select\n\t" + select
    from_sql = "\n\tfrom " + from_table
    if where:
        where_sql = "\n\twhere " + where
    else:
        where_sql = ""
    if group_by:
        group_by_sql = "\n\tgroup by " + group_by
    else:
        group_by_sql = ""
    if order_by:
        order_by_sql = "\n\torder by " + order_by
    else:
        order_by_sql = ""
    if limit:
        limit_sql = "\n\tlimit " + str(int(limit))
    else:
        limit_sql = ""
    sql = head + drop_sql + create + select_sql + from_sql + where_sql + group_by_sql + order_by_sql + limit_sql
    return sql + "\n\t;\n"


def save_to_file(file, contents, mode='w', encoding='utf-8'):
    """
    保存到本地文件
    :param file: str, 本地文件名
    :param contents: str, 待保存的对象
    :param mode: str, 打开文件的模式, 'w'为新建或覆盖, 'a'为新建或追加
    :param encoding: str, 编码方式
    :return: 本地file文件
    """
    fh = open(file, mode=mode, encoding=encoding)
    fh.write(contents)
    fh.close()


def create_matching_sql(df, create_table, from_table, location=None, limit=None, head="", name=None, equal=None,
                        white=None, black=None, lower=None, upper=None):
    """
    基于规则表生成标签匹配HiveSQL
    :param df: DataFrame, 匹配规则表
    :param create_table: str, 数据库新建表名称，用于存放查询结果
    :param location: str, 数据库新建表的存储路径
    :param from_table: str, 数据库用来匹配标签的原始表名称
    :param limit: int, HiveSQL脚本中的limit取值
    :param head: str, 系统设置语句
    :param name: {str: str}, 数据库新增的标签字段名：规则表相应的标签值字段名
    :param equal: {str: str}, 数据库执行相等规则字段名：规则表相应的相等规则字段名
    :param white: {str: str}, 数据库执行白名单规则字段名：规则表相应的白名单规则字段名
    :param black: {str: str}, 数据库执行黑名单规则字段名：规则表相应的黑名单规则字段名
    :param lower: list, 执行SQL时需要先将取值转成小写再执行规则的字段名，默认为空
    :param upper: list, 执行SQL需要先将取值转成大写再执行规则的字段名，默认为空
    :return: str, HiveSQL脚本
    """
    # 生成别名
    if name:
        as_sql = df[list(set(name.values()))].apply(
            lambda x: " ".join(["\"" + str(x[name[col]]) + "\" as " + str(col) + "," for col in name.keys()]), axis=1)
    else:
        as_sql = ""

    # 基于取值相等进行筛选
    if equal:
        where_hql1 = df[list(set(equal.values()))].apply(
            lambda x: "and ".join([str(col) + "= \"" + str(x[equal[col]]) + "\"" for col in equal.keys()]), axis=1)
    else:
        where_hql1 = ""

    # 基于黑白规则正则表达式进行筛选
    if not white:
        white = dict()
    if not black:
        black = dict()
    columns = list(set(list(white.values()) + list(black.values())))
    if columns:
        where_hql2 = df[columns].apply(func_where_sql, axis=1, white=white, black=black, lower=lower, upper=upper)
    else:
        where_hql2 = ""

    if equal and columns:
        where_hql = where_hql1 + " and " + where_hql2  # 相等及正则两种筛选条件都有时
    else:
        where_hql = where_hql1 + where_hql2  # 最多只有一种筛选条件时

    drop_sql = "\ndrop table if exists " + create_table + ";\n"
    if location:
        create_select_sql = "create table " + create_table + " stored as orcfile location '" + location \
                            + "' as\nselect distinct a.*"
    else:
        create_select_sql = "create table " + create_table + " stored as orcfile as\nselect distinct a.*"
    if limit:
        sub_table = "select " + as_sql + " * from " + from_table + " where " + where_hql + " limit " + str(int(limit))
    else:
        sub_table = "select " + as_sql + " * from " + from_table + " where " + where_hql
    from_sql = "\nfrom\n\t(\n\t" + " union all\n\t".join(sub_table) + "\n\t) a;\n"

    sql = head + drop_sql + create_select_sql + from_sql
    return sql


def type_decode(df, coding, code, decode):
    """
    对列值进行解码，即把取值为代码的列解码成相应的实际名称，解码结果以新的列追加到原表末尾
    :param df: DataFrame, 含有待解码列的数据表
    :param coding: DataFrame, 编码-解码表
    :param code: list, 需要解码的列名
    :param decode: dict, 编码-解码表中的对应关系，{编码字段名：相应解码字段名}
    :return: DataFrame
    """
    if len(code) != len(decode):
        print("\n编码与解码字段个数不匹配，请重新输入\n")
        return df
    decode_key = list(decode.keys())
    decode_value = list(decode.values())
    # 检查唯一性
    coding_tmp = coding[decode_key + decode_value].drop_duplicates()

    check_code = coding_tmp.groupby(decode_key)[decode_value].count().reset_index()
    check_code = check_code[check_code[decode_value].sum(1) > len(decode_value)]
    if len(check_code) > 0:
        print("\n编码与解码为一对多，不满住唯一性，请检查:\n")
        print(check_code)

    check_decode = coding_tmp.groupby(decode_value)[decode_key].count().reset_index()
    check_decode = check_decode[check_decode[decode_key].sum(1) > len(decode_key)]
    if len(check_decode) > 0:
        print("\n解码与编码值为一对多，不满住唯一性，请检查:\n")
        print(check_decode)

    # 开始解码
    coding_tmp.columns = code + decode_value
    df2 = df.merge(coding_tmp, how='left', on=code)
    df_check = df2[code + decode_value].drop_duplicates()
    df_check = df_check[df_check.isna().sum(1) > 0]
    if len(df_check) > 0:
        print("\n注意，存在解码失败，请检查：\n")
        print(df_check)
    return df2


def df_to_excels(df, excel_name, sheet_name=None, path_output=None, show=True):
    """
    基于指定的列值将数据拆分保存成excel，或拆分excel及sheet, 各列的取值将作为拆分后的excel或sheet名称
    :param df: DataFrame, 待拆分的数据表名称
    :param excel_name: str, 需要按取值拆分成不同excel的列名
    :param sheet_name: str, 需要按取值拆分成不同sheet的列名
    :param path_output: str, 生成的excel保存的路径
    :param show: boolean, 是否打印拆分过程
    :return: 本地excel文件
    """
    if not path_output:
        path_output = './output/'  # 没有指定输出路径时，输出至默认路径下的output文件夹
    if not os.path.exists(path_output):
        os.mkdir(path_output)  # 输出路径不存在时直接创建确保路径存在

    # 两层循环，第一层循环拆分生成excel工作簿
    for comp in df[excel_name].drop_duplicates():
        output_file = path_output + str(comp) + '.xlsx'
        pd.DataFrame().to_excel(output_file, index=False)
        writer = pd.ExcelWriter(output_file)
        df_excel = df[df[excel_name] == comp]
        if show:
            print(comp)
        if sheet_name:
            # 第二层循环拆分生成sheet工作表，保存到相应公司的工作簿中
            for bran in df_excel[sheet_name].drop_duplicates():
                df_sheet = df_excel[df_excel[sheet_name] == bran]
                df_sheet.to_excel(writer, index=False, sheet_name=str(bran))
                if show:
                    print('\t', bran)
        else:
            df_excel.to_excel(writer, index=False, sheet_name=str(comp))
        writer.save()
        writer.close()
    if show:
        print('\n拆分完成！')


def left_fill_value(df, fill, inplace=True):
    """
    将数据表指定的列向左用指定的字符进行填充, 该函数主要针对0开头的数字字符，如'0016', 在数据读入时被转化成数值16,因此需要用0进行填充还原。
    :param df: DataFrame, 数据表名称
    :param fill: {str:(int, str)}, 填充方式说明，{需要填充的列名： (最终要填充达到的位数, 用来填充的字符)}
    :param inplace: bool, 是否替换原来的数据表
    :return: DataFrame
    """
    if inplace:
        for col in fill.keys():
            df[col] = df[col].apply(lambda x: str(x).rjust(fill[col][0], fill[col][1]))
    else:
        df2 = df.copy()
        for col in fill.keys():
            df2[col] = df2[col].apply(lambda x: str(x).rjust(fill[col][0], fill[col][1]))
        return df2


def clean_excel_sample(df, path, primary, white, black=None, keep_na=None, inplace=True, fill=None, path_white=None,
                       path_black=None, show=True, reason=True, default=True):
    """
    样本清洗筛选函数。基于规则表对相应的本地excel文件中的各个sheet表的数据进行清洗筛选
    :param df: DataFrame, 清洗规则表
    :param path: str, 待清洗筛选的本地excel文件路径
    :param primary: dict('excel': str, 'sheet': str), {'excel': 规则表中相应的列名, 'sheet': 规则表中相应的列名}
    :param white: dict(str: str), {需要执行白规则的sheet列名：规则表中相应的白规则列名}
    :param black: {str: str}, {需要执行黑规则的sheet列名：规则表中相应的黑规则列名}
    :param keep_na: list(str), 取值为空时输出到白名单的列名，如果不在keep_na中，则默认输出到黑名单
    :param inplace: bool, 是否覆盖原始数据。清洗过程中需要对列取值字符化处理，默认字符化结果直接覆盖原始值
    :param fill: dict(str: (int, str)), 字符化填充说明，{需要填充的列名： (最终要填充达到的位数, 用来填充的字符)}
    :param path_white: str, 白名单输出路径
    :param path_black: str, 黑名单输出路径
    :param show: bool, 是否打印清洗进度
    :param reason: bool, 输出的黑名单是否添加剔除原因
    :param default: bool, 黑白规则均无命中情况时是否默认判定为黑名单
    :return: 清洗完的本地excel文件（黑白名单）
    """
    if not white and not black:
        print('请设置相应的清洗规则')
        return
    if not black:
        black = dict()
    if not keep_na:
        keep_na = list()
    # 输出路径准备，用于存放清洗结果
    if path_white:
        if not os.path.exists(path_white):
            os.mkdir(path_white)
    else:
        path_white = path + 'white/'
        if not os.path.exists(path_white):
            os.mkdir(path_white)
    if path_black:
        if not os.path.exists(path_black):
            os.mkdir(path_black)
    else:
        path_black = path + 'black/'
        if not os.path.exists(path_black):
            os.mkdir(path_black)
    # 三层循环，第一层遍历excel，第二层遍历excel中的每一个sheet，第三层遍历执行每个sheet相应的清洗规则
    excel_files = [f[:-5] for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and (f[-4:] == 'xlsx')]
    excel_rules = df[primary['excel']].unique()  # 有清洗规则的excel明细
    # 第一层循环, 遍历excel
    for excel in excel_files:
        # 检查待清洗的excel是否存在相应的清洗规则，若无规则则跳过清洗下一个excel
        if excel not in excel_rules:
            if show:
                print("\n%s\t不在清洗规则表中, 清洗跳过" % excel)
            continue
        if show:  # 打印清洗进度
            print("\n%s\t%s" % (excel, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

        # 输出文件初始化
        file_white = path_white + excel + '_white.xlsx'
        file_black = path_black + excel + '_black.xlsx'
        writer_white = pd.ExcelWriter(file_white)
        writer_black = pd.ExcelWriter(file_black)

        reader_raw = pd.ExcelFile(path + excel + '.xlsx')

        df_rule_excel = df[(df[primary['excel']] == excel)]  # excel相应的清洗规则表

        sheet_files = reader_raw.sheet_names  # 待清洗的sheet明细
        sheet_rules = df_rule_excel[primary['sheet']].unique()  # 有清洗规则的sheet明细

        # 第二层循环, 遍历sheet
        for sheet in sheet_files:
            # 检查待清洗的sheet是否存在相应的清洗规则，若无规则则跳过清洗下一个sheet
            if sheet not in sheet_rules:
                if show:
                    print("\t%s %s: 不在清洗规则表中, 清洗跳过" % (excel, sheet))
                continue

            # 读入待清洗的sheet数据
            df_raw = pd.read_excel(reader_raw, sheet)

            # 数据预处理
            if inplace:
                # 对指定的列进行填充补齐，缺失的地方默认仍保持缺失
                if fill:
                    for col in fill.keys():
                        df_raw[col] = df_raw[col].apply(lambda x: str(x).rjust(fill[col][0], fill[col][1]))
                # 提取需要执行清洗规则的列并转成字符型
                df_raw_str = df_raw[list(set(list(white.keys()) + list(black.keys())))].astype('object')
            else:
                df_raw_str = df_raw.copy()
                if fill:
                    for col in fill.keys():
                        df_raw_str[col] = df_raw_str[col].apply(lambda x: str(x).rjust(fill[col][0], fill[col][1]))
                df_raw_str = df_raw_str[list(set(list(white.keys()) + list(black.keys())))].astype('object')

            # 输出数据表初始化准备
            df_white = pd.DataFrame()
            df_black = pd.DataFrame()

            # 第三层循环，遍历执行相应的清洗规则
            df_rule_sheet = df_rule_excel[df_rule_excel[primary['sheet']] == sheet]  # sheet相应的清洗规则表
            for j in range(len(df_rule_sheet)):
                # 清洗算法：
                # 第一步：基于规则black、white参数生成相应的布尔索引，基于keep_na参数对原始数据的na导致的布尔索引中的na进行填充,
                #        在keep_na的填True, 不在则默认填False
                # 第二步：合成布尔索引逻辑，确定命中的名单，对于每个样本，所有black为False且所有的white为True判为白名单，
                #        至少一个black为True判为黑，否则判为灰
                # 第三步：对于黑名单，基于reason参数确定是否在输出的数据表后追加命中原因（用0-1矩阵表示各规则命中情况）。
                # 第四步：判断灰名单是否非空 且 还有规则未执行，是 则基于灰名单继续执行下一轮循环（第一至第四步），否 则执行下一步
                # 第五步：判断灰名单是否非空，是则执行第六步，否则执行第七步
                # 第六步：基于unknown参数判断灰名单是否应纳入白名单，默认纳入黑名单，黑名单则基于reason参数确定在输出的数据表后
                #        追加命中原因（规则原因记为'unmatch',取值为1)
                # 第七步：输出黑白名单
                rules = df_rule_sheet.iloc[j]
                df_bool_white = pd.DataFrame()
                for key in white.keys():
                    if key in keep_na:  # na需要保留，等价于命中白名单
                        df_bool_white[key + '_white'] = df_raw_str[key].str.contains(rules[white[key]],
                                                                                     flags=re.IGNORECASE, na=True)
                    else:
                        df_bool_white[key + '_white'] = df_raw_str[key].str.contains(rules[white[key]],
                                                                                     flags=re.IGNORECASE, na=False)
                s_bool_white = df_bool_white.mean(1) == 1  # 合并索引，所有white均为True时才判定为白
                if black:
                    df_bool_black = pd.DataFrame()
                    for key in black.keys():
                        if key in keep_na:  # na需要保留，等价于没有命中黑名单
                            df_bool_black[key + '_black'] = df_raw_str[key].str.contains(rules[black[key]],
                                                                                         flags=re.IGNORECASE, na=False)
                        else:
                            df_bool_black[key + '_black'] = df_raw_str[key].str.contains(rules[black[key]],
                                                                                         flags=re.IGNORECASE, na=True)
                    s_bool_white = np.logical_and(s_bool_white, df_bool_black.sum(1) == 0)
                    # 合并索引，所有black均为False时才判定为白
                    s_bool_black = df_bool_black.sum(1) > 0  # 合并索引，有一个black为True时则判定为黑

                    if s_bool_black.sum() > 0:
                        df_black_tmp = df_raw.loc[s_bool_black[s_bool_black].index]  # 当前清洗规则所命中的黑名单

                        if reason:  # 是否增加被判为黑的原因
                            df_black_tmp[df_bool_black.columns] = df_bool_black[s_bool_black].astype(int)

                        df_black = df_black.append(df_black_tmp)  # 累计命中的黑名单
                        df_raw_str.drop(df_black_tmp.index, inplace=True)  # 剔除黑名单，剩下的为灰名单

                if s_bool_white.sum() > 0:
                    df_white_tmp = df_raw.loc[s_bool_white[s_bool_white].index]  # 当前清洗规则所命中的白名单
                    df_white = df_white.append(df_white_tmp)  # 累计命中的白名单
                    df_raw_str.drop(df_white_tmp.index, inplace=True)  # 剔除白名单，剩下的为灰名单

                if len(df_raw_str) != 0 and (j < len(df_rule_sheet) - 1):  # 判断是否还有灰名单需要清洗 且 还有清洗规则未执行
                    continue
                elif len(df_raw_str) != 0:
                    if default:  # 判断是否将灰名单纳入白名单
                        df_white = df_white.append(df_raw.loc[df_raw_str.index])
                    else:  # 默认判为黑名单
                        df_black_tmp = df_raw.loc[df_raw_str.index]
                        if reason:
                            df_black_tmp['default_black'] = 1
                        df_black = df_black.append(df_black_tmp)
                else:
                    pass

            if show:
                count_raw = len(df_raw)
                count_white = len(df_white)
                count_black = len(df_black)
                print("\t%s:共%d行，其中clean:black = %.1f%% : %.1f%% = %d : %d" %
                      (sheet, count_raw, (100*count_white/count_raw), (100*count_black/count_raw),
                       count_white, count_black))

            if len(df_white) > 0:
                df_white.to_excel(writer_white, sheet_name=sheet, index=None)
            if len(df_black) > 0:
                df_black.to_excel(writer_black, sheet_name=sheet, index=None)
        try:
            writer_white.save()
            writer_white.close()
            writer_black.save()
            writer_black.close()
        except Exception:
            print('--------------出错了----------------')
            print('traceback.print_exc():')
            print(traceback.print_exc())


def excel_to_df(excel, col_type='str', show=True):
    """
    将excel文件中的所有sheet工作表合并成一个DataFrame
    :param excel: str, 含有绝对路径及文件后缀的Excel文件，如d:/folder_name/file_name.xlsx
    :param col_type: str or dict, 各字段数据类型
    :param show: bool, 是否打印过程
    :return: DataFrame
    """
    file = pd.ExcelFile(excel)
    sheet_names = file.sheet_names
    df = pd.DataFrame()
    for sheet in sheet_names:
        df = df.append(pd.read_excel(file, sheet_name=sheet, dtype=col_type))
        if show:
            print('\t', sheet)
    return df


def excels_to_df(path, col_type='str', show=True):
    """
    将路径中所有excel文件的所有sheet工作表合并成一个DataFrame
    :param path: str, Excel文件所在路径，如d:/folder_name/
    :param col_type: str or dict, 各字段数据类型
    :param show: boolean, 是否打印过程
    :return: DataFrame
    """
    # 两层循环，第一层遍历Excel工作簿，第二层遍历工作簿里面的sheet
    df = pd.DataFrame()
    excels = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and (f[-4:] == 'xlsx')]
    for excel in excels:
        if show:
            print(excel)
        excel_file = pd.ExcelFile(path + excel)
        sheet_names = excel_file.sheet_names
        for sheet in sheet_names:
            if show:
                print('\t', sheet)
            df = df.append(pd.read_excel(excel_file, sheet_name=sheet, dtype=col_type))
    return df


def format_adjust(df, base, transpose, sep="|", name=None):
    """
    列装置（针对一对多情况）
    :param df: DataFrame, 数据表
    :param base: list, 基础列列名
    :param transpose: str, 需要转置列的列名
    :param sep : str, 转置后用于连接的分隔符
    :param name : str, 装置后的列名
    :return: DataFrame
    """
    df2 = df[list(set(base + [transpose]))].groupby(base).apply(lambda x: sep .join(x[transpose]))
    df2 = df2.reset_index()
    if name:
        df2.columns = base + [name]
    else:
        df2.columns = base + [transpose]
    return df2


def year_month_to_date(df, year='年', month='月'):
    """
    将年和月两列合并成datetime.date类型的日期（格式为yyyy-mm-dd）
    :param df: DataFrame, 数据表
    :param year: str, 年份所在的列名
    :param month: str, 月份所在的列名
    :return: Series
    """
    date = df[year].astype(str) + df[month].apply(lambda x: str(x).rjust(2, '0'))
    return date.apply(lambda x: dt.datetime.strptime(x, "%Y%m").date())


def year_week_to_date(df, year='年', week='周'):
    """
    将年和周两列合并成datetime.date类型的日期（格式为yyyy-mm-dd）
    :param df: DataFrame
    :param year: str, df表中的年份字段名
    :param week: str, df表中的周序字段名，定义方法为每年的1月1日起，每7天为一周
    :return: Series
    """
    return df[[year, week]].apply(lambda x:  dt.date(int(x[year]), 1, 1) + dt.timedelta(7*(int(x[week]) - 1)), 1)


def statistic_monthly(df, brand_range, left_on, right_on, statistic, date_in, date_out, date='日期', keep=True):
    """
    交易流水筛选汇总：月度-->月度
    :param df: DataFrame, 原始流水统计表
    :param brand_range:  DataFrame, 品牌对应关键字表
    :param left_on: list, 左表（df)连接键
    :param right_on: list, 右表(brand_range)连接键
    :param statistic: list, 需要进行统计处理的指标
    :param date_in: str, 纳入日期字段名，日期数据类型为yyyymmdd整型
    :param date_out: str, 剔除日期字段名，日期数据类型为yyyymmdd整型
    :param date: str, df表中的日期字段名，数据类型为datetime.date
    :param keep: bool, 是否输出合并前及剔除后的品牌数据
    :return: [DataFrame, DataFrame] 品牌层面及公司层面的流水统计表
    """
    # 表格合并
    df.sort_values(left_on + [date], inplace=True)
    tmp = df.merge(
        brand_range[right_on + [date_in, date_out]],
        how='left',
        left_on=left_on,
        right_on=right_on)
    # 品牌日期范围筛选
    tmp[date_in].fillna(value=0, inplace=True)
    tmp[date_out].fillna(value=29999999, inplace=True)

    tmp['date_int'] = tmp[date].apply(lambda x: x.year * 10000 + x.month * 100 + x.day)  # 日期转成整型表示
    df_select = tmp[(tmp['date_int'] >= tmp[date_in]) & (tmp['date_int'] < tmp[date_out])]
    # 品牌汇总
    if keep:
        brand_statistic = df[left_on + [date] + statistic]
    else:
        brand_statistic = df_select[left_on + [date] + statistic]
    brand_statistic.index = range(len(brand_statistic))
    # 公司汇总
    company_statistic = df_select.groupby([left_on[0], date])[statistic].sum()
    company_statistic = company_statistic.reset_index()
    company_statistic.insert(1, left_on[1], company_statistic[left_on[0]].apply(lambda x: x + '_合并'))
    return brand_statistic, company_statistic


def statistic_weekly(df, brand_range, left_on, right_on, statistic, date_in, date_out, year='年', week='周', keep=True):
    """
    交易流水筛选汇总：周度-->周度
    :param df: DataFrame, 原始流水统计表
    :param brand_range:  DataFrame, 品牌对应关键字表
    :param left_on:  list, 左表（df)连接键
    :param right_on:  list, 右表(brand_range)连接键
    :param statistic:  list, 需要进行统计处理的指标
    :param date_in: str, 纳入日期字段名，日期数据类型为yyyymmdd整型
    :param date_out: str, 剔除日期字段名，日期数据类型为yyyymmdd整型
    :param year: str, df表中的年份字段名
    :param week: str, df表中的周序字段名，培训方法为每年的1月1日起，每7天为一周
    :param keep: bool, 是否输出合并前的品牌数据
    :return: DataFrame, DataFrame 品牌层面及公司层面的流水统计表
    """
    df2 = df.copy()
    df2['日期'] = year_week_to_date(df, year=year, week=week)
    brand_statistic, company_statistic = statistic_monthly(df=df2, brand_range=brand_range, left_on=left_on,
                                                           right_on=right_on, statistic=statistic, date_in=date_in,
                                                           date_out=date_out, keep=keep)
    return brand_statistic, company_statistic


def get_quarter(df, date='日期'):
    """
    将yyyy-mm-dd格式的日度日期转成yyyyqq格式的季度日期
    :param df: DataFrame
    :param date: str, 日期所在的列名，列值必须是datetime.date类型
    :return: Series
    """
    return df[date].apply(lambda x: str(x.year)) + df[date].apply(lambda x: str((x.month-1)//3 + 1).rjust(2, 'Q'))


def get_period(df, df_key, company_data='公司', company_key='公司', period_start='财报周期起始月份', date='日期'):
    """
    根据财报周期的起始日期划分财报周期
    :param df: DataFrame
    :param df_key: DataFrame
    :param company_data: str
    :param company_key: str
    :param period_start: int
    :param date: date
    :return:
    """
    df_key2 = df_key[[company_key, period_start]].copy()
    company_check = [i for i in df[company_data].unique() if i not in df_key2[company_key].unique()]
    if len(company_check) > 0:
        print("以下公司的财报周期起始月份缺少记录\n\t%s" % company_check)
        df_key2 = df_key2.append(pd.DataFrame({company_key: company_check, period_start: 1}))
    df_key2 = df_key2[df_key2[company_key].isin(df[company_data].unique())].sort_values([company_key, period_start])
    na_check = df_key2.groupby(company_key).count()
    na_check = na_check[na_check[period_start] == 0]
    if len(na_check) > 0:
        print("以下公司的财报周期起始月份为空值：\n\t%s" % na_check.index.values)
        df_key2.loc[df_key2[company_key].isin(na_check.index.values), [period_start]] = 1
    df_key2.fillna(method='ffill', inplace=True)
    value_check = df_key2.groupby(company_key).max().merge(df_key2.groupby(company_key).mean(), 'left', left_index=True,
                                                           right_index=True)
    value_check = value_check[value_check[period_start + '_x'] != value_check[period_start + '_y']]
    if len(value_check) > 0:
        print("以下公司有多个不同的财报周期起始月份：\n\t%s" % value_check.index.values)
    df_period_start = df_key2.groupby(company_key).first()
    df2 = df.merge(df_period_start, 'left', left_on=company_data, right_index=True)
    df2[period_start] = df2[period_start].astype(int)

    def period_get(x, d=date, s=period_start):
        if x[d].month < x[s]:
            return str(x[d].year - 1) + str((x[d].month + 12 - x[s])//3 + 1).rjust(2, 'p')
        else:
            return str(x[d].year) + str((x[d].month - x[s])//3 + 1).rjust(2, 'p')
    return df2[[date, period_start]].apply(period_get, 1)


def statistic_merge(df, statistic, group):
    """
    聚合统计
    :param df: DataFrame, 数据表
    :param statistic: list(str), 聚合统计的统计指标，默认常见的交易指标
    :param group: list(str), 聚合统计的基础粒度，默认对公司在季度上做聚合
    :return: DataFrame
    """
    df2 = df.groupby(group)[statistic].sum()
    for i in range(len(group)):
        df2.insert(i, group[i], df2.index.get_level_values(group[i]).values)
    df2.index = range(len(df2))
    return df2


def data_masking(df, masking):
    """
    对数据表指定列进行相应数量级的脱敏处理
    :param df: DataFrame, 待脱敏的数据表
    :param masking: dic(str: int), {待脱敏的列名: 相应的脱颖数量级}
    :return: DataFrame
    """
    dic_masking = {10: '(十位脱敏)', 100: '(百位脱敏)', 1000: '(千位脱敏)', 10000: '(万位脱敏)', 100000: '(十万位脱敏)',
                   1000000: '(百万位脱敏)', 10000000: '(千万位脱敏)', 100000000: '(亿位脱敏)', 1000000000: '(十亿位脱敏)', }
    columns = list(masking.keys())
    levels = list(masking.values())
    df_masking = df.copy()
    df_masking.drop(columns=columns, inplace=True)
    for i in range(len(columns)):
        column = columns[i]
        level = levels[i]
        df_masking[column + dic_masking[level]] = level * ((df[column]/level).round(0))
    return df_masking


def excels_masking(path, masking, path_output=None, show=True, dtype='object'):
    """
    将路径中所有excel文件的所有sheet工作表的指定列进行相应数量级的数据脱敏
    :param path: str, 含有绝对路径及文件后缀的Excel文件，如d:/folder_name/file_name.xlsx
    :param masking: dic, 待脱敏的列名及相应的脱颖数量级
    :param path_output: str, 脱敏数据输出保存路径
    :param show: boolean, 是否打印过程
    :param dtype: str or dict, 导入数据表字段数据类型说明
    :return 本地excel文件
    """
    # 两层循环，第一层遍历Excel工作簿，第二层遍历工作簿里面的sheet, 对数据表进行脱敏
    if not path_output:
        path_output = path + 'output/'
    if not os.path.exists(path_output):
        os.mkdir(path_output)
    excels = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and (f[-4:] == 'xlsx')]
    for excel in excels:
        if show:
            print(excel)
        reader = pd.ExcelFile(path + excel)
        writer = pd.ExcelWriter(path_output + excel)
        sheet_names = reader.sheet_names
        for sheet in sheet_names:
            if show:
                print('\t', sheet)
            df_masking = data_masking(pd.read_excel(reader, sheet_name=sheet, dtype=dtype), masking=masking)
            df_masking.to_excel(writer, index=False, sheet_name=sheet)
        writer.save()
        writer.close()


def count_line(file, encoding='utf-8', m=10, show=True):
    """
    计算文件有多少行数据
    :param file: str, 待抽样的文件名，含路径及后缀
    :param encoding: str, 编码方式
    :param m: int, 每次读入处理的数据量，单位为兆
    :param show: bool, 是否打印中间过程
    :return: int, 行数，统计结果
    """
    f = open(file, 'r', encoding=encoding)
    count = 0
    while True:
        chunk = f.read(1024 * 1024 * m)  # 每次读入part M
        if not chunk:
            break
        count += chunk.count('\n')
    f.close()
    if show:
        print("# 该文件共有%d行 #" % count)
    return count


def line_sample(file, encoding=None, m=20, n=5000):
    """
    行抽样
    :param file: str, 待抽样的文件名，含路径及后缀
    :param encoding: str, 编码方式
    :param m: int, 每次读入处理的数据量，单位为兆
    :param n: int, 抽样数量
    :return: 抽样结果，本地文件
    """
    # 提取文件路径及文件名
    file_name = str(os.path.basename(file).split('.')[0])
    file_path = os.path.dirname(file) + '/'

    # 计算文件的总行数
    count = 0
    f = open(file, 'r', encoding=encoding)
    while True:
        chunk = f.read(1024 * 1024 * m)  # 每次读入m M
        if not chunk:
            break
        count += chunk.count('\n')
    f.close()

    # 情况一：样本总数不多于抽样个数时，直接返回全部样本
    if count <= n:
        print("\n%s: 抽样数%d ≥ 总行数%d，无需抽样" % (file_name, n, count))
        return
    else:
        print("\n%s: 共有%d行，约%.1fM，开始抽样..." % (file_name, count, os.path.getsize(file)/1024/1024))
    # 情况二：样本总数超过抽样个数时，基于随机序号进行抽样
    int_range = [i for i in range(count)]  # 产生序号
    random.shuffle(int_range)  # 打散序号
    sample_range = sorted(int_range[:n])  # 抽取前n个随机样本序号

    # 遍历文件，每次读取一部分，基于随机样本序号进行抽样
    i = 0  # 块数计数
    j = 0  # 行数计数
    tail = ''  # 分块残留数据尾部
    df_sample = pd.DataFrame()  # 用于存放抽样结果
    f = open(file, 'r', encoding=encoding)
    while True:
        read = f.read(1024 * 1024 * m)
        if j >= count or not read:
            break
        i += 1
        print("%s\t处理%s第%d部分" % (dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), file_name, i))
        chunk = tail + read
        lines = chunk.splitlines()
        if chunk[-1] == '\n':  # 数据块最后一行是否完整切断
            df_chunk = pd.DataFrame(lines)[0].str.split(',', expand=True)
            tail = ''
        else:
            df_chunk = pd.DataFrame(lines[:-1])[0].str.split(',', expand=True)
            tail = lines[-1]
        chunk_len = len(df_chunk)
        df_chunk.index = range(j, j + chunk_len)
        # 当前样本序号范围包含抽样序号则抽样，否则跳过
        sample_id = list(set(range(j, j + chunk_len)) & set(sample_range))  # 序号交集
        if len(sample_id) > 0:
            df_sample = df_sample.append(df_chunk.loc[sample_id])
        j += chunk_len
    f.close()
    # 抽样结果输出保存
    if len(df_sample) > 0:
        try:
            df_sample.to_csv(file_path + 'sample_' + file_name + '.txt', index=False, header=False)
            print("======%s\t%s处理完毕，原文件共有%d行，成功抽样%d行======" % (
                dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), file_name, count, len(df_sample)))
        except Exception:
            print(traceback.print_exc())
    else:
        print("======%s\t%s处理完毕，原文件共有%d行，抽取样本0行======" % (
        dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), file_name, count))


def merchant_split(file, encoding=None, m=20, city_cd_loc=4, in_rule='^[1-9]|0156|000[01]',
                   out_rule='^0(?!00[01]|156)'):
    """
    商户按地区拆分境内外
    :param file: str, 待抽样的文件名，含路径及后缀
    :param encoding: str, 编码方式
    :param m: int, 每次读入处理的数据量，单位为兆
    :param city_cd_loc: int, 城市代码字段所在的位置，从0开始，例如在第五列，则输入4
    :param in_rule: str, 城市代码为境内的正则表达式
    :param out_rule: str, 城市代码为境外的正则表达式
    :return: 拆分结果，本地文件
    """
    # 提取文件路径及文件名
    file_name = str(os.path.basename(file).split('.')[0])
    file_path = os.path.dirname(file) + '/'

    # 初始化输出对象

    if os.path.exists(file_path + file_name + '_domestic.txt'):
        os.remove(file_path + file_name + '_domestic.txt')
    if os.path.exists(file_path + file_name + '_international.txt'):
        os.remove(file_path + file_name + '_international.txt')
    if os.path.exists(file_path + file_name + '_remained.txt'):
        os.remove(file_path + file_name + '_remained.txt')
    print("\n%s: 约%.1fM，开始处理..." % (file_name, os.path.getsize(file) / 1024 / 1024))
    # 遍历文件，每次读取一部分数据，基于city_code进行地区划分
    i = 0  # 分块计数
    count = 0  # 总行数
    count_in = 0  # 境内数据行数
    count_out = 0  # 境外数据行数
    count_remain = 0  # 无法判断境内外数据行数
    tail = ''  # 分块残留数据尾部
    f = open(file, 'r', encoding=encoding)
    while True:
        read = f.read(1024 * 1024 * m)
        if not read:
            break
        i += 1
        print("%s\t处理%s第%d部分" % (dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), file_name, i))
        chunk = tail + read
        lines = chunk.splitlines()
        if chunk[-1] == '\n':
            df_chunk = pd.DataFrame(lines)[0].str.split(',', expand=True)
            tail = ''
        else:
            df_chunk = pd.DataFrame(lines[:-2])[0].str.split(',', expand=True)
            tail = lines[-1]
        count += len(df_chunk)
        # 划分境内外地区
        df_chunk_in = df_chunk[df_chunk[city_cd_loc].str.contains(in_rule, flags=re.IGNORECASE)]
        df_chunk_out = df_chunk[df_chunk[city_cd_loc].str.contains(out_rule, flags=re.IGNORECASE)]
        if len(df_chunk_in) > 0:
            count_in += len(df_chunk_in)
            df_chunk.drop(df_chunk_in.index, inplace=True)
            df_chunk_in.to_csv(file_path + file_name + '_domestic.txt', index=False, header=False, mode='a')  # 追加写入

        if len(df_chunk_out) > 0:
            count_out += len(df_chunk_out)
            df_chunk.drop(df_chunk_out.index, inplace=True)
            df_chunk_out.to_csv(file_path + file_name + '_international.txt', index=False, header=False, mode='a')  # 追加

        if len(df_chunk) > 0:
            count_remain += len(df_chunk)
            df_chunk.to_csv(file_path + file_name + '_remained.txt', index=False, header=False, mode='a')  # 追加
    f.close()
    print("======%s\t%s处理完毕，共有%d行，其中domestic:international:remained =  %.1f%% : %.1f%% : %.1f%% = %d : %d : %d"
          % (dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), file_name, count, (100 * count_in / (count + 0.001)),
             (100 * count_out / (count + 0.001)), (100 * count_remain / (count + 0.001)), count_in, count_out, count_remain))


def industry_merchant_clean(file, columns, df_rule, encoding=None, m=20, show=True, keyword=False):
    """
    商户清洗函数，根据规则对商户文本文件进行清洗
    :param file: str, 待抽样的文件名，含路径及后缀
    :param columns: list, 文件列名
    :param df_rule: DataFrame, 清洗规则文件
    :param encoding: str, 编码方式
    :param m: int, 每次读入处理的数据量，单位为兆
    :param show: bool, 是否打印中间过程
    :param keyword: bool, clean文件是否增加一列name_white
    :return: 清洗结果，本地文件
    """
    # 提取文件路径及文件名
    file_name = str(os.path.basename(file).split('.')[0])
    file_path = os.path.dirname(file) + '/'

    # 匹配待清洗商户相应的规则
    df_rule_industry = df_rule[df_rule['file_name'] == file_name]
    if len(df_rule_industry) == 0:
        print('\n%s: 找不到清洗规则，清洗跳过！' % file_name)
        return

    # 输出路径初始化
    path_clean = file_path + 'clean/'
    path_black = file_path + 'black/'
    if not os.path.exists(path_clean):
        os.mkdir(path_clean)
    if not os.path.exists(path_black):
        os.mkdir(path_black)
    if show:
        print("\n%s: 约%.1fM，开始清洗..." % (file_name, os.path.getsize(file) / 1024 / 1024))
    # 情况二：样本总数超过抽样个数时，基于随机序号进行抽样
    # 遍历文件，每次读取一部分，基于随机样本序号进行抽样
    f = open(file, 'r', encoding=encoding)
    i = 0  # 块数计数
    tail = ''  # 分块残留数据尾部
    while True:
        read = f.read(1024 * 1024 * m)
        if not read:
            break
        i += 1
        print("%s\t处理%s第%d部分" % (dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), file_name, i))
        chunk = tail + read
        lines = chunk.splitlines()
        if chunk[-1] == '\n':  # 数据块最后一行是否完整切断
            df_chunk = pd.DataFrame(lines)[0].str.split(',', expand=True)
            tail = ''
        else:
            df_chunk = pd.DataFrame(lines[:-1])[0].str.split(',', expand=True)
            tail = lines[-1]
        df_chunk.columns = columns
        # 执行清洗循环，第一层遍历每个地区（境内+境外），提取相应的清洗规则（可能存在多个规则）
        for district in df_rule_industry['district'].unique():
            # 创建数据集存放当前地区的清洗结果
            df_clean = pd.DataFrame()
            df_black = pd.DataFrame()

            df_rule_district = df_rule_industry[df_rule_industry['district'] == district]
            # 第二层循环在地区内执行相应的清洗规则
            for j in range(len(df_rule_district)):
                count_raw = len(df_chunk)
                # 规则一：城市白名单
                city_code_white = df_rule_district['citycode_white'].iloc[j]
                df_city_code = df_chunk[df_chunk['city_cd'].str.contains(city_code_white, flags=re.IGNORECASE)]
                df_chunk.drop(df_city_code.index, inplace=True)  # 城市白名单匹配失败的，等待执行下一轮清洗规则或输出为未匹配

                # 规则二：商户名称白名单
                name_white = df_rule_district['name_white'].iloc[j]
                df_name_white = df_city_code[
                    df_city_code['mchnt_name'].str.contains(name_white, flags=re.IGNORECASE)]
                if keyword:
                    df_name_white['keywords'] = name_white
                df_city_code.drop(df_name_white.index, inplace=True)
                if len(df_city_code) > 0:
                    df_city_code['drop_reason'] = '不在name_white内'
                    df_black = df_black.append(df_city_code)

                # 规则三：商户名称黑名单
                name_black = df_rule_district['name_black'].iloc[j]
                df_name_black = df_name_white[
                    ~df_name_white['mchnt_name'].str.contains(name_black, flags=re.IGNORECASE)]
                df_name_white.drop(df_name_black.index, inplace=True)
                if len(df_name_white) > 0:
                    df_name_white['drop_reason'] = 'name_black'
                    df_black = df_black.append(df_name_white)

                # 规则四：商户类型
                mcc_white = df_rule_district['mcc_white'].iloc[j]
                df_mcc_white = df_name_black[df_name_black['mcc'].str.contains(mcc_white, flags=re.IGNORECASE)]
                df_name_black.drop(df_mcc_white.index, inplace=True)
                if len(df_name_black) > 0:
                    df_name_black['drop_reason'] = 'MCC 不在范围内'
                    df_black = df_black.append(df_name_black)

                df_clean = df_clean.append(df_mcc_white)  # 保留当前规则清洗后的商户

            count_clean = len(df_clean)
            count_black = len(df_black)
            count_unmatch = len(df_chunk)
            if show:
                print("\t清洗%s，共%d行，其中clean:black:unmatch = %.1f%% : %.1f%% : %.1f%% = %d : %d : %d"
                      % (district, count_raw, (100 * count_clean / (count_raw + 0.001)),
                         (100 * count_black / (count_raw + 0.001)),
                         (100 * count_unmatch / (count_raw + 0.001)), count_clean, count_black, count_unmatch))
            if count_clean > 0:
                df_clean.to_csv(path_clean + file_name + '_' + str(district) + '_clean.txt', mode='a', index=None,
                                header=False)
            if count_black > 0:
                df_black.to_csv(path_black + file_name + '_' + str(district) + '_black.txt', mode='a', index=None,
                                header=False)
            if count_unmatch > 0:
                df_chunk.to_csv(path_black + file_name + '_' + str(district) + '_unmatch.txt', mode='a', index=None,
                                header=False)
    f.close()


def str_replace(df, columns, str_raw="(", str_rep="\\\\("):
    """
    替换指定列中的指定字符
    :param df: DataFrame
    :param columns: list, 可能含有需要替换字符的列名
    :param str_raw: str, 需要被替换的原始字符
    :param str_rep: 需要替换成的目标字符
    :return: DataFrame
    """
    for col in columns:
        df[col] = df[col].apply(lambda x: x.replace(str_raw, str_rep))
    return df


def company_file_rule_check(rule, path):
    """
    检查是否有公司缺少清洗规则
    :param rule: DataFrame, 清洗规则表
    :param path: str, 以公司名称命名的Excel文件所在路径
    :return: (print)
    """
    excel_files = pd.Series([f[:-5] for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and (f[-4:] == 'xlsx')])
    check = excel_files[~excel_files.isin(rule['company'].unique())]
    if len(check) > 0:
        print("\n注意！下列Excel文件名与清洗规则表的公司名未匹配上\n", check)
    else:
        print("检查完毕，待清洗的公司文档集合中未发现有公司缺清洗规则")
