# -*- coding:utf8 -*-
# !/usr/bin/env python3

import pymysql 

CONFIG = {
    'host': '192.168.20.42',
    'user': 'root',
    'passwd': '123123',
    'db': 'test',
    'port': 3306,
    'charset': 'utf8'
}


class SqlLib:
    def __init__(self, host=CONFIG['host'], user=CONFIG['user'], passwd=CONFIG['passwd'],
                 db=CONFIG['db'], port=CONFIG['port'], charset=CONFIG['charset']):
        self.db = pymysql.Connect(host=host, user=user, passwd=passwd, db=db, port=port, charset=charset)
        self.cur = self.db.cursor()

    def update(self, sql_cmd):
        try:
            self.cur.execute(sql_cmd)
            self.db.commit()
        except Exception as e:
            print("sql Error:" + sql_cmd)
            print(e)
            print("sql Error End")
            self.db.rollback()
            self.close()
            raise e

    def save(self, table, info_dict, field_list):
        """
        保存数据
        table: 数据表名
        info_dict: 保存数据 key,value字典
        field_list: where条件字段列表
        """
        select_info_dict = self._dict_format(info_dict, field_list)
        if self._select(table, select_info_dict, field_list):
            cmd = 'update {table} set {set} where {where}'.format(table=table,
                                                                  set=self._dict_covert(',', info_dict, field_list),
                                                                  where=self._dict_covert('and', select_info_dict))
            print(cmd)                                                     
        else:
            cmd = 'insert into {table} ({keys}) values ("{values}")'.format(table=table,
                                                                            keys=','.join(info_dict.keys()),
                                                                            values='", "'.join(info_dict.values()))
        try:
            self.cur.execute(cmd)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            self.close()
            raise e
    def insert(self, table, info_dict):
        """
        保存数据
        table: 数据表名
        info_dict: 保存数据 key,value字典
        field_list: where条件字段列表
        """
        cmd = 'insert into {table} ({keys}) values ("{values}")'.format(table=table,
                                                                            keys=','.join(info_dict.keys()),
                                                                            values='", "'.join(info_dict.values()))
        try:
            self.cur.execute(cmd)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            self.close()
            raise e
    def _select(self, table, info_dict, field_list):
        """
        查询数据
        table: 数据表名
        info_dict: 查询where条件key,value字典
        field_list: 需要查询的字段列表（默认：所有字段）
        :return: 查询结果
        """
        field = ', '.join(field_list) if field_list else '*'
        cmd = 'select {field} from {table} where {where}'.format(field=field, table=table,
                                                                 where=self._dict_covert('and', info_dict))
        self.cur.execute(cmd)
        return self.cur.fetchall()

    def select(self, sql_cmd):
        """
        查询数据
        执行查询语句
        """
        self.cur.execute(sql_cmd)
        return self.cur.fetchall()

    def _dict_covert(self, join_str, info_dict, field_list=None):
        result = ""
        for k, v in info_dict.items():
            if field_list and k in field_list:
                continue
            result += '{0}="{1}"'.format(k, v) if not result else ' {0} {1}="{2}"'.format(join_str, k, v)
        return result

    def _dict_format(self, info_dict, key_list):
        result = {}
        for key in key_list:
            result[key] = info_dict[key]
        return result

    def close(self):
        self.db.close()


if __name__ == "__main__":
    sql = SqlLib()
    info_dict = {
        "commit_hash": '123456',
        "author": '白白白',
        "is_trigger": 'False',
    }
    sql.save('field_ci', info_dict, ['commit_hash', 'branch'])