def duration_rec(engine_db, engine_elastic, start_point, end_point, epg_table_name, rabbit_host, exchange_name, routing_key_name, time_sleep):
    # ______Package_______
    import pandas as pd
    from datetime import datetime
    from datetime import timedelta
    import time
    import json
    from sqlalchemy import create_engine
    from elasticsearch import Elasticsearch
    import jdatetime

    engine = create_engine(engine_db)
    es = Elasticsearch(hosts=engine_elastic)

    # ______Read event  & epg file_______

    s_point = start_point
    d_point = end_point

    print('-----Duration GET-----')
    time.sleep(time_sleep)


    for day in range(0, 365):
        t1 = time.perf_counter()
        start = datetime.strptime(s_point, '%Y-%m-%dT%H:%M')
        start = start + timedelta(days=day)
        end = start + timedelta(minutes=30)
        _start = start - timedelta(days=1)

        day = datetime.strftime(start, '%Y-%m-%dT%H:%M')
        end_day = datetime.strftime(end, '%Y-%m-%dT%H:%M')
        lable = datetime.strftime(start, '%d_%m_%Y')

        # print(read_file)

        try:

            _end = datetime.strftime(end, '%Y-%m-%d')
            _start_ = datetime.strftime(_start, '%Y-%m-%d')

            body = {'query': {'bool': {'must': [{'match_all': {}},
                                                {'range': {'@timestamp': {'gte': '{}T21:35:00.000Z'.format(_start_),
                                                                          'lt': '{}T21:25:00.000Z'.format(_end)}}}]}}}

            # body = {'query': {'bool': {'must': [{'match_all': {}},
            #                                     {'range': {'timestamp': {'gte': '2021-12-27T00:05:00.000Z',
            #                                                               'lt': '2021-12-27T23:55:00.000Z'}}}]}}}

            def process_hits(hits):
                _df1 = pd.DataFrame()
                for item in hits:
                    _res = json.dumps(item, indent=2)
                    _df0 = pd.DataFrame(item['_source'], columns=item['_source'].keys(), index=[0])
                    # print(_df0)
                    _df1 = _df1.append(_df0, ignore_index=True)
                return _df1

            data = es.search(
                index='live-action',
                scroll='1m',
                size=10000,
                body=body
            )

            sid = data['_scroll_id']
            scroll_size = len(data['hits']['hits'])
            sum = 0
            read_file = pd.DataFrame()
            while scroll_size > 0:
                _a = process_hits(data['hits']['hits'])
                read_file = read_file.append(_a, ignore_index=True)

                data = es.scroll(scroll_id=sid, scroll='2m')

                sid = data['_scroll_id']

                scroll_size = len(data['hits']['hits'])

                read_file = read_file[
                    ['time_stamp', '@version', 'sys_id', 'time_code', '@timestamp', 'service_id', 'session_id',
                     'content_name', 'channel_name', 'content_type_id', 'action_id']]

                read_file = read_file.astype(str)

                sum = scroll_size + sum
                print(sum)
            read_file["time_stamp"] = pd.to_datetime(read_file["time_stamp"])
            # read_file['time_stamp2'] = read_file['time_stamp'] + pd.Timedelta(hours=4, minutes=30)
            read_file['time_stamp'] = read_file['time_stamp'] + pd.Timedelta(hours=3, minutes=30)
            read_file['time_stamp'] = read_file['time_stamp'].apply(
                lambda x: pd.Timestamp(x).strftime('%Y-%m-%dT%H:%M:%S'))
            # read_file['time_stamp2'] = read_file['time_stamp2'].apply(lambda x: pd.Timestamp(x).strftime('%Y-%m-%dT%H:%M:%S'))
            read_file.to_csv(r'F:\clean_epg\epg\____epg____{}.csv'.format(lable), index=False)
            # read_file=pd.read_csv(r'F:\clean_epg\epg\____epg____{}.csv', index_col=False)

            exist = 1
        except:
            # print('nok1')
            exist = 0
        if exist != 0:
            try:

                epg = pd.read_excel(r'F:\clean_epg\epg\epg_{}.xlsx'.format(lable), index_col=False)
                epg['channel'] = epg['channel'].fillna('_بدون عنوان')
                print('ok2')

                # v_q = datetime.strftime(end, "%Y-%m-%d")
                # my_str = "\'" + v_q + "\'"
                # epg = pd.read_sql_query('SELECT * FROM {x} where "e_dete" = date {y}'.format(x=epg_table_name, y=my_str),con=engine.connect())
                # print(epg['s_time'])
                # epg["s_time"]=epg["s_time"].astype(str)
                # epg["e_time"] = epg["e_time"].astype(str)

                exist = 1
            except:

                exist = 0
            if exist != 0:
                # ______Epg Correction_________
                epg['visit'] = 0
                epg['u_visit'] = 0
                epg['dur'] = 0
                epg["s1_time"] = pd.to_datetime(epg["s_time"])
                epg["e1_time"] = pd.to_datetime(epg["e_time"])
                _column = epg["e1_time"] - epg["s1_time"]
                epg['dif'] = _column.dt.total_seconds() / 60

                epg['s_time'] = [x[:5] for x in epg['s_time']]
                epg['s_time'] = epg['s_time'].astype(str) + ':00'
                epg['e_time'] = [x[:5] for x in epg['e_time']]
                epg['e_time'] = epg['e_time'].astype(str) + ':00'

                # ________event file Correction_______

                df = read_file.copy()
                df['action_id'] = df['action_id'].astype(str)

                # ______Create dfn DataFrame______

                dfn = df.copy()
                dfn['tag'] = ''
                d = ''
                dfx = dfn.copy()
                dfn = dfx.copy()
                epgt = epg.copy()
                epg['tag'] = ''

                for i in range(0, len(dfn)):

                    print(i)
                    a = dfn.loc[i, 'session_id']
                    # print(a)

                    tag = dfn.loc[i, 'tag']
                    chan = dfn.loc[i, 'channel_name']

                    if a != '' and tag == '':

                        dfa = dfn[dfn['session_id'] == a]
                        dfa = dfa.sort_values(by='time_stamp')

                        # print(dfa)
                        dfa['tag'] = 'tg'
                        dfn.update(dfa)

                        flag = 1
                        k = 0
                        m = 0
                        fg = 0
                        fhc = 0
                        fhd = 0
                        fv = 1
                        ind1_o = -1
                        ind0_s = 4e6

                        for r in range(0, len(dfa)):

                            a_ch = dfa.iloc[r]['channel_name']
                            # print(a_ch)
                            epg_c = epg[epg['channel'] == a_ch]
                            # print(epg_c)

                            if r == 0:
                                a_ch1 = a_ch
                                a_i = 0

                            if a_ch == a_ch1:
                                a_ch1 = a_ch
                                a_flag = 0

                            else:
                                df_a = dfa.iloc[a_i:r][:]
                                a_ch1 = a_ch
                                a_i = r
                                a_flag = 1

                                s_flag = 1
                                play_flag = 0
                                pu_flag = 0
                                e_fill = 0
                                s_fill = 0
                                for act_id in range(0, len(df_a)):
                                    # print('1st')

                                    play = df_a.iloc[act_id]['action_id']

                                    if play != '2' and s_flag == 1:
                                        t_play = df_a.iloc[act_id]['time_stamp'][:16]
                                        tt_play = datetime.strptime(t_play, '%Y-%m-%dT%H:%M')
                                        t_play = tt_play + timedelta(hours=0, minutes=0, seconds=0)

                                        play_flag = 1
                                        s_flag = 0
                                        pu_flag = 0
                                        s_fill = 1

                                    if play == '2' and play_flag == 1:
                                        t_pause = df_a.iloc[act_id]['time_stamp'][:16]
                                        tt_pause = datetime.strptime(t_pause, '%Y-%m-%dT%H:%M')
                                        t_pause = tt_pause + timedelta(hours=0, minutes=0, seconds=0)

                                        s_flag = 1
                                        pu_flag = 1
                                        play_flag = 0
                                        e_fill = 1
                                    if pu_flag == 0 and act_id == len(df_a) - 1:
                                        try:
                                            t_pause = dfa.iloc[r]['time_stamp'][:16]
                                            tt_pause = datetime.strptime(t_pause, '%Y-%m-%dT%H:%M')
                                            t_pause = tt_pause + timedelta(hours=0, minutes=0, seconds=0)
                                            e_fill = 1

                                        except:
                                            t_pause = dfa.iloc[act_id]['time_stamp'][:16]
                                            tt_pause = datetime.strptime(t_pause, '%Y-%m-%dT%H:%M')
                                            t_pause = tt_pause + timedelta(hours=0, minutes=30, seconds=0)
                                            e_fill = 1
                                    if s_fill == 1 and e_fill == 1:

                                        s_fill = 0
                                        e_fill = 0
                                        ind1_o = -1
                                        ind0_s = 4e6
                                        e_flag = 1

                                        ch_name = df_a.iloc[act_id]['channel_name']
                                        # print(ch_name)
                                        try:
                                            epgy = epg[epg['channel'] == ch_name]
                                        except:
                                            e_flag = 0

                                        if len(epgy) != 0 and e_flag == 1:

                                            t_play = t_play.strftime('%H:%M:%S')
                                            t_pause = t_pause.strftime('%H:%M:%S')

                                            try:
                                                st = epgy[(epgy['s_time'] <= t_play) & (epgy['e_time'] >= t_play)]

                                                if len(st) == 0 and len(
                                                        epgy[(epgy['s_time'] <= t_pause) & (
                                                                epgy['s_time'] >= t_play)]) != 0:
                                                    st_h = epgy[
                                                        (epgy['s_time'] <= t_pause) & (epgy['s_time'] >= t_play)]
                                                    st = st_h
                                                    t_play = st_h.iloc[0]['s_time']

                                            except:
                                                pass

                                            try:
                                                ind0 = (st.index.values)[0]

                                            except:
                                                ind0 = 30000

                                                pass

                                            try:
                                                en = epgy[(epgy['s_time'] <= t_pause) & (epgy['e_time'] >= t_pause)]

                                                ind_h = 0
                                                if len(en) == 0 and len(
                                                        epgy[(epgy['e_time'] <= t_pause) & (
                                                                epgy['s_time'] >= t_play)]) != 0:
                                                    en_h = epgy[
                                                        (epgy['e_time'] <= t_pause) & (epgy['s_time'] >= t_play)]
                                                    en = en_h
                                                    t_pause = en_h.iloc[len(en) - 1]['e_time']
                                                    ind_h = (en.index.values)[-1]
                                                elif len(en) == 0 and len(
                                                        epgy[
                                                            (epgy['e_time'] <= t_pause) & (
                                                                    epgy['e_time'] >= t_play)]) != 0:
                                                    en_h = epgy[
                                                        (epgy['e_time'] <= t_pause) & (epgy['e_time'] >= t_play)]
                                                    # print('en_h')
                                                    # print(en_h)
                                                    en = en_h
                                                    t_pause = en_h.iloc[len(en) - 1]['e_time']
                                                    ind_h = (en.index.values)[-1]

                                            except:

                                                pass

                                            try:
                                                ind1 = (en.index.values)[0]
                                                if ind_h > ind1:
                                                    ind1 = ind_h
                                                    ind_h = 0
                                                else:
                                                    ind_h = 0


                                            except:
                                                ind1 = 5e5
                                                pass

                                            try:
                                                epgx = epgy[(epgy.index >= ind0) & (epgy.index <= ind1)]
                                                epgx['tag'] = 'tg'
                                                epg['tag'].update(epgx['tag'])
                                            except:
                                                pass

                                            if ind0 < ind0_s:
                                                ind0_s = ind0

                                            if ind1 > ind1_o:
                                                ind1_o = ind1

                                            if len(epgx) > 1:

                                                ex = datetime.strptime(epg.loc[ind0, 'e_time'], '%H:%M:%S')
                                                cx = datetime.strptime(t_play, '%H:%M:%S')

                                                try:
                                                    __column = ((ex - cx).total_seconds()) / 60
                                                    epg.loc[ind0, 'dur'] = epg.loc[ind0, 'dur'] + __column
                                                except:
                                                    print('__column')

                                            if ind0 == ind1:
                                                cx = datetime.strptime(t_play, '%H:%M:%S')
                                                dx = datetime.strptime(t_pause, '%H:%M:%S')

                                                __column = ((dx - cx).total_seconds()) / 60
                                                epg.loc[ind1, 'dur'] = epg.loc[ind0, 'dur'] + __column

                                            else:
                                                try:
                                                    ey = datetime.strptime(epg.loc[ind1, 's_time'], '%H:%M:%S')
                                                    dx = datetime.strptime(t_pause, '%H:%M:%S')
                                                    __column = ((dx - ey).total_seconds()) / 60
                                                    epg.loc[ind1, 'dur'] = epg.loc[ind1, 'dur'] + __column
                                                except:
                                                    print('__column')

                                            if len(epgx) > 2:
                                                epg.loc[ind0 + 1:ind1 - 1, 'dur'] = epg.loc[ind0 + 1:ind1 - 1,
                                                                                    'dur'] + epg.loc[
                                                                                             ind0 + 1:ind1 - 1,
                                                                                             'dif']

                            if r == len(dfa) - 1 and a_flag == 1:
                                # print('2nd')

                                df_a = dfa.iloc[r:len(dfa)][:]

                                s_flag = 1
                                play_flag = 0
                                pu_flag = 0
                                e_fill = 0
                                s_fill = 0
                                for act_id in range(0, len(df_a)):

                                    play = df_a.iloc[act_id]['action_id']

                                    if play != '2' and s_flag == 1:
                                        t_play = df_a.iloc[act_id]['time_stamp'][:16]
                                        tt_play = datetime.strptime(t_play, '%Y-%m-%dT%H:%M')
                                        t_play = tt_play + timedelta(hours=0, minutes=0, seconds=0)

                                        play_flag = 1
                                        s_flag = 0
                                        pu_flag = 0
                                        s_fill = 1

                                    if play == '2' and play_flag == 1:
                                        t_pause = df_a.iloc[act_id]['time_stamp'][:16]
                                        tt_pause = datetime.strptime(t_pause, '%Y-%m-%dT%H:%M')
                                        t_pause = tt_pause + timedelta(hours=0, minutes=0, seconds=0)

                                        s_flag = 1
                                        pu_flag = 1
                                        play_flag = 0
                                        e_fill = 1
                                    if pu_flag == 0 and act_id == len(df_a) - 1:
                                        try:
                                            t_pause = dfa.iloc[r]['time_stamp'][:16]
                                            tt_pause = datetime.strptime(t_pause, '%Y-%m-%dT%H:%M')

                                            tp_flag = datetime.strptime(day, '%Y-%m-%dT%H:%M')

                                            if tt_pause > tp_flag:
                                                t_pause = datetime.strptime(end_day, '%Y-%m-%dT%H:%M')
                                            else:
                                                t_pause = tt_pause + timedelta(hours=0, minutes=30, seconds=0)

                                            e_fill = 1

                                        except:
                                            t_pause = dfa.iloc[act_id]['time_stamp'][:16]
                                            tt_pause = datetime.strptime(t_pause, '%Y-%m-%dT%H:%M')

                                            tp_flag = datetime.strptime(day, '%Y-%m-%dT%H:%M')

                                            if tt_pause > tp_flag:
                                                t_pause = datetime.strptime(end_day, '%Y-%m-%dT%H:%M')
                                            else:
                                                t_pause = tt_pause + timedelta(hours=0, minutes=30, seconds=0)

                                            e_fill = 1

                                    if s_fill == 1 and e_fill == 1:

                                        s_fill = 0
                                        e_fill = 0
                                        ind1_o = -1
                                        ind0_s = 4e6
                                        e_flag = 1

                                        ch_name = df_a.iloc[act_id]['channel_name']
                                        try:
                                            epgy = epg[epg['channel'] == ch_name]
                                        except:
                                            e_flag = 0

                                        if len(epgy) != 0 and e_flag == 1:
                                            t_play = t_play.strftime('%H:%M:%S')
                                            t_pause = t_pause.strftime('%H:%M:%S')

                                            try:
                                                st = epgy[(epgy['s_time'] <= t_play) & (epgy['e_time'] >= t_play)]

                                                if len(st) == 0 and len(
                                                        epgy[(epgy['s_time'] <= t_pause) & (
                                                                epgy['s_time'] >= t_play)]) != 0:
                                                    st_h = epgy[
                                                        (epgy['s_time'] <= t_pause) & (epgy['s_time'] >= t_play)]

                                                    st = st_h

                                                    t_play = st_h.iloc[0]['s_time']

                                            except:
                                                pass

                                            try:
                                                ind0 = (st.index.values)[0]

                                            except:
                                                ind0 = 30000

                                                pass

                                            try:
                                                en = epgy[(epgy['s_time'] <= t_pause) & (epgy['e_time'] >= t_pause)]

                                                ind_h = 0
                                                if len(en) == 0 and len(
                                                        epgy[(epgy['e_time'] <= t_pause) & (
                                                                epgy['s_time'] >= t_play)]) != 0:
                                                    en_h = epgy[
                                                        (epgy['e_time'] <= t_pause) & (epgy['s_time'] >= t_play)]
                                                    en = en_h
                                                    t_pause = en_h.iloc[len(en) - 1]['e_time']

                                                    ind_h = (en.index.values)[-1]
                                                elif len(en) == 0 and len(
                                                        epgy[
                                                            (epgy['e_time'] <= t_pause) & (
                                                                    epgy['e_time'] >= t_play)]) != 0:
                                                    en_h = epgy[
                                                        (epgy['e_time'] <= t_pause) & (epgy['e_time'] >= t_play)]
                                                    # print('en_h')
                                                    # print(en_h)
                                                    en = en_h
                                                    t_pause = en_h.iloc[len(en) - 1]['e_time']
                                                    ind_h = (en.index.values)[-1]

                                            except:

                                                pass

                                            try:
                                                ind1 = (en.index.values)[0]
                                                if ind_h > ind1:
                                                    ind1 = ind_h
                                                    ind_h = 0
                                                else:
                                                    ind_h = 0

                                            except:
                                                ind1 = 5e5

                                                pass

                                            try:
                                                epgx = epgy[(epgy.index >= ind0) & (epgy.index <= ind1)]
                                                epgx['tag'] = 'tg'
                                                epg['tag'].update(epgx['tag'])
                                            except:
                                                pass

                                            if ind0 < ind0_s:
                                                ind0_s = ind0

                                            if ind1 > ind1_o:
                                                ind1_o = ind1

                                            if len(epgx) > 1:
                                                ex = datetime.strptime(epg.loc[ind0, 'e_time'], '%H:%M:%S')
                                                cx = datetime.strptime(t_play, '%H:%M:%S')

                                                try:
                                                    __column = ((ex - cx).total_seconds()) / 60
                                                    epg.loc[ind0, 'dur'] = epg.loc[ind0, 'dur'] + __column
                                                except:
                                                    print('__column')

                                            if ind0 == ind1:
                                                cx = datetime.strptime(t_play, '%H:%M:%S')
                                                dx = datetime.strptime(t_pause, '%H:%M:%S')

                                                __column = ((dx - cx).total_seconds()) / 60
                                                epg.loc[ind1, 'dur'] = epg.loc[ind0, 'dur'] + __column

                                            else:
                                                try:
                                                    ey = datetime.strptime(epg.loc[ind1, 's_time'], '%H:%M:%S')
                                                    dx = datetime.strptime(t_pause, '%H:%M:%S')
                                                    __column = ((dx - ey).total_seconds()) / 60
                                                    epg.loc[ind1, 'dur'] = epg.loc[ind1, 'dur'] + __column
                                                except:
                                                    print('__column')

                                            if len(epgx) > 2:
                                                epg.loc[ind0 + 1:ind1 - 1, 'dur'] = epg.loc[ind0 + 1:ind1 - 1,
                                                                                    'dur'] + epg.loc[
                                                                                             ind0 + 1:ind1 - 1,
                                                                                             'dif']

                            if r == len(dfa) - 1 and a_flag == 0:
                                # print('3rd')

                                df_a = dfa.iloc[a_i:len(dfa)][:]
                                # print(df_a)
                                s_flag = 1
                                play_flag = 0
                                pu_flag = 0
                                e_fill = 0
                                s_fill = 0
                                for act_id in range(0, len(df_a)):

                                    play = df_a.iloc[act_id]['action_id']

                                    if play != '2' and s_flag == 1:
                                        t_play = df_a.iloc[act_id]['time_stamp'][:16]
                                        tt_play = datetime.strptime(t_play, '%Y-%m-%dT%H:%M')
                                        t_play = tt_play + timedelta(hours=0, minutes=0, seconds=0)
                                        # print(t_play)

                                        play_flag = 1
                                        s_flag = 0
                                        pu_flag = 0
                                        s_fill = 1

                                    if play == '2' and play_flag == 1:
                                        t_pause = df_a.iloc[act_id]['time_stamp'][:16]
                                        tt_pause = datetime.strptime(t_pause, '%Y-%m-%dT%H:%M')
                                        t_pause = tt_pause + timedelta(hours=0, minutes=0, seconds=0)
                                        # print(t_pause)

                                        s_flag = 1
                                        pu_flag = 1
                                        play_flag = 0
                                        e_fill = 1
                                    if pu_flag == 0 and act_id == len(df_a) - 1:
                                        try:
                                            t_pause = dfa.iloc[r]['time_stamp'][:16]
                                            tt_pause = datetime.strptime(t_pause, '%Y-%m-%dT%H:%M')

                                            tp_flag = datetime.strptime(day, '%Y-%m-%dT%H:%M')
                                            if tt_pause > tp_flag:
                                                t_pause = datetime.strptime(end_day, '%Y-%m-%dT%H:%M')
                                            else:
                                                t_pause = tt_pause + timedelta(hours=0, minutes=30, seconds=0)
                                            # print(t_pause)

                                            e_fill = 1

                                        except:
                                            t_pause = dfa.iloc[act_id]['time_stamp'][:16]
                                            tt_pause = datetime.strptime(t_pause, '%Y-%m-%dT%H:%M')

                                            tp_flag = datetime.strptime(day, '%Y-%m-%dT%H:%M')

                                            if tt_pause > tp_flag:
                                                t_pause = datetime.strptime(end_day, '%Y-%m-%dT%H:%M')
                                            else:
                                                t_pause = tt_pause + timedelta(hours=0, minutes=30, seconds=0)

                                            # print(t_pause)

                                            e_fill = 1

                                    if s_fill == 1 and e_fill == 1:

                                        s_fill = 0
                                        e_fill = 0
                                        ind1_o = -1
                                        ind0_s = 4e6
                                        e_flag = 1
                                        ch_name = df_a.iloc[act_id]['channel_name']
                                        # print(ch_name)
                                        try:
                                            epgy = epg[epg['channel'] == ch_name]
                                        except:
                                            e_flag = 0

                                        if len(epgy) != 0 and e_flag == 1:

                                            e_flag = 1
                                            t_play = t_play.strftime('%H:%M:%S')
                                            # print(t_play)
                                            t_pause = t_pause.strftime('%H:%M:%S')

                                            try:
                                                st = epgy[(epgy['s_time'] <= t_play) & (epgy['e_time'] >= t_play)]
                                                # print('st')
                                                # print(len(st))
                                                # print(len(epgy[(epgy['s_time'] <= t_pause) & (epgy['s_time'] >= t_play)]))
                                                if len(st) == 0 and len(
                                                        epgy[(epgy['s_time'] <= t_pause) & (
                                                                epgy['s_time'] >= t_play)]) != 0:
                                                    st_h = epgy[
                                                        (epgy['s_time'] <= t_pause) & (epgy['s_time'] >= t_play)]
                                                    # print(st_h)
                                                    st = st_h

                                                    t_play = st_h.iloc[0]['s_time']

                                                    # print(t_play)

                                            except:
                                                pass

                                            try:
                                                ind0 = (st.index.values)[0]
                                                # print('ind0')
                                                # print(ind0)
                                            except:
                                                ind0 = 30000
                                                # print(ind0)
                                                pass

                                            try:
                                                en = epgy[(epgy['s_time'] <= t_pause) & (epgy['e_time'] >= t_pause)]
                                                # print('en')
                                                # print(len(en))
                                                # print(en)
                                                # print(len(epgy[(epgy['e_time'] <= t_pause) & (epgy['e_time'] >= t_play)]))
                                                ind_h = 0
                                                if len(en) == 0 and len(
                                                        epgy[(epgy['e_time'] <= t_pause) & (
                                                                epgy['e_time'] >= t_play)]) != 0:
                                                    en_h = epgy[
                                                        (epgy['e_time'] <= t_pause) & (epgy['e_time'] >= t_play)]
                                                    en = en_h
                                                    t_pause = en_h.iloc[len(en) - 1]['e_time']
                                                    # print(t_pause)
                                                    ind_h = (en.index.values)[-1]
                                                elif len(en) == 0 and len(
                                                        epgy[
                                                            (epgy['e_time'] <= t_pause) & (
                                                                    epgy['e_time'] >= t_play)]) != 0:
                                                    en_h = epgy[
                                                        (epgy['e_time'] <= t_pause) & (epgy['e_time'] >= t_play)]
                                                    # print('en_h')
                                                    # print(en_h)
                                                    en = en_h
                                                    t_pause = en_h.iloc[len(en) - 1]['e_time']
                                                    ind_h = (en.index.values)[-1]

                                            except:

                                                pass

                                            try:
                                                ind1 = (en.index.values)[0]
                                                if ind_h > ind1:
                                                    ind1 = ind_h
                                                    ind_h = 0
                                                else:
                                                    ind_h = 0

                                            except:

                                                ind1 = 5e5
                                                pass

                                            try:
                                                epgx = epgy[(epgy.index >= ind0) & (epgy.index <= ind1)]
                                                epgx['tag'] = 'tg'
                                                epg['tag'].update(epgx['tag'])

                                            except:
                                                pass
                                            try:
                                                if ind0 < ind0_s:
                                                    ind0_s = ind0
                                                    # print('ind0-s')
                                                    # print(ind0_s)
                                            except:
                                                pass

                                            try:
                                                if ind1 > ind1_o:
                                                    ind1_o = ind1
                                                    # print('ind1-o')
                                                    # print(ind1_o)
                                            except:
                                                pass

                                            if len(epgx) > 1:
                                                ex = datetime.strptime(epg.loc[ind0, 'e_time'], '%H:%M:%S')
                                                cx = datetime.strptime(t_play, '%H:%M:%S')

                                                try:
                                                    __column = ((ex - cx).total_seconds()) / 60
                                                    epg.loc[ind0, 'dur'] = epg.loc[ind0, 'dur'] + __column
                                                except:
                                                    print('__columne')

                                            try:
                                                if ind0 == ind1:
                                                    cx = datetime.strptime(t_play, '%H:%M:%S')
                                                    dx = datetime.strptime(t_pause, '%H:%M:%S')

                                                    __column = ((dx - cx).total_seconds()) / 60
                                                    epg.loc[ind1, 'dur'] = epg.loc[ind0, 'dur'] + __column

                                                else:
                                                    try:
                                                        ey = datetime.strptime(epg.loc[ind1, 's_time'], '%H:%M:%S')
                                                        dx = datetime.strptime(t_pause, '%H:%M:%S')
                                                        __column = ((dx - ey).total_seconds()) / 60
                                                        epg.loc[ind1, 'dur'] = epg.loc[ind1, 'dur'] + __column
                                                    except:
                                                        print('__column')

                                                if len(epgx) > 2:
                                                    epg.loc[ind0 + 1:ind1 - 1, 'dur'] = epg.loc[ind0 + 1:ind1 - 1,
                                                                                        'dur'] + epg.loc[
                                                                                                 ind0 + 1:ind1 - 1,
                                                                                                 'dif']


                                            except:
                                                pass
                        epg.loc[(epg['tag'] == 'tg'), 'visit'] += 1

                        epg['tag'] = ''
                        ex = 0
                        cx = 0
                        dx = 0
                        ey = 0

                epg = epg.drop(
                    columns=['ID_Kind', 'channel2', 'ID_Program', 's_date', 's1_time', 'e_time', 'e1_time', 'dif',
                             'tag', 's_time', 'e_dete'])

                epg["Time_Play_x"] = pd.to_datetime(epg["Time_Play"])
                epg['Time_Play_x'] = epg['Time_Play_x'] - pd.Timedelta(hours=3, minutes=30)
                epg['Time_Play_x'] = epg['Time_Play_x'].apply(lambda x: pd.Timestamp(x).strftime('%Y-%m-%dT%H:%M:%S'))
                epg['j_Time_Play'] = jdatetime.date.fromgregorian(day=int(lable.split('_')[0]),
                                                                  month=int(lable.split('_')[1]),
                                                                  year=int(lable.split('_')[2]))
                epg['j_Time_Play'] = epg['j_Time_Play'].astype(str)
                epg.to_excel(r'F:\clean_epg\epg___{}.xlsx'.format(lable), index=False, encoding='utf-8')
                epg.to_sql('duration', engine.connect(), if_exists='replace', index=False)
                epg = epg.astype(str)

                # **********************************************
                #
                import pika
                import json
                import ast

                credentials = pika.PlainCredentials(username='admin', password='R@bbitMQ1!')
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=rabbit_host, port=5672, credentials=credentials))

                channel = connection.channel()

                # channel.queue_declare(queue='durvisit', durable=True)

                d_epg = epg.to_dict('records')

                for q in d_epg:
                    msg2 = q
                    print(msg2)
                    channel.basic_publish(exchange=exchange_name, routing_key=routing_key_name,
                                          properties=pika.BasicProperties(content_type='application/json'),
                                          body=json.dumps(msg2, ensure_ascii=False))

                connection.close()
        t2 = time.perf_counter()

        dt21 = t2 - t1
        loop = 86400
        from datetime import datetime
        from datetime import timedelta
        ct = end
        ct = ct + timedelta(days=2)
        tz = datetime.now()
        if tz > ct:
            res = 10
        else:
            s_tz = tz.second + tz.minute * 60 + tz.hour * 3600
            res = 88800 - s_tz
        print(res)
        print('---------------------------------------------')
        time.sleep(res)

