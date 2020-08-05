# coding:utf-8
'''
Created on Jul 14, 2020

@author: mizuno
'''

import pandas as pd
import numpy as np
import datetime as dt
import time
import concurrent.futures
from concurrent.futures.process import ProcessPoolExecutor

class Wifilog_lib :

    def __init__(self, df_ap, df_data):
        self.df_ap = df_ap
        self.df_data = df_data
        self.aps = self.df_ap.AP.values.tolist() #讓呎ｺ悶Μ繧ｹ繝医↓螟画鋤縺励※縺翫￥
        self.aps.append('out')
        #self.aps = np.append(self.aps, 'out') #譛�蠕後↓out繧定ｿｽ蜉�
        self.transitions = [[0 for i in self.aps] for i in self.aps] #�ｼ呈ｬ｡蜈�驟榊�励ｒ菴懈��
        self.transition_from = []
        self.transition_to = []
        self.duration = []
        self.roop_index = 0

    def getDuplicate(self, df, column_name):
        return df.drop_duplicates(subset = column_name)

    def doTask(self, i):
        start = time.time()
        '''
        20200329螟画峩
        transitions縺ｸ縺ｮ逋ｻ骭ｲ縺ｯtransiton_from, transition_to縺九ｉ蠕後〒菴懈�舌☆繧�
        duration_ap縺ｯtransition_from縺ｨ蜷後§縺ｪ縺ｮ縺ｧ蜑企勁縺吶ｋ
        Mznizu02縺ｮtransitions縺ｮ縺ｨ縺ｯ邨先棡縺悟ｾｮ螯吶↓驕輔≧(譎る俣縺ｧ荳ｦ縺ｳ譖ｿ縺医◆縺ｮ縺ｧ縲∵耳遘ｻ縺碁��縺ｫ縺ｪ縺｣縺ｦ縺�繧矩Κ蛻�縺後≠繧�)
        邱乗焚縺ｯ豁｣縺励＞縺ｮ縺ｧ縲∝�ｨ菴薙�ｯ豁｣縺励＞
        '''
        #繝ｭ繝ｼ繧ｫ繝ｫ螟画焚縺ｮ螳夂ｾｩ
        transition_from = []
        transition_to = []
        duration = []

#        from_ap = self.aps.index('out') #out縺ｮ隕∫ｴ�逡ｪ蜿ｷ繧貞叙蠕�
        to_ap = -1
        for j in self.pdf.itertuples(): #1陦後★縺､蜈ｨ繝ｭ繧ｰ繝√ぉ繝�繧ｯ
            if (j.client == i): #蛻ｩ逕ｨ閠�縺ｮMac繧｢繝峨Ξ繧ｹ縺瑚ｦ九▽縺九▲縺溷�ｴ蜷�
                #譛�蛻昴↓隕九▽縺九▲縺溯｡後�ｮAP縺ｮ隕∫ｴ�逡ｪ蜿ｷ繧貞ｾ励ｋ
                #print("Mac逋ｺ隕�:{}".format(time.time()-start))
                for k in self.aps : #to縺ｮAP縺ｮ隕∫ｴ�逡ｪ蜿ｷ繧呈爾縺�
                    if (j.AP == k):
                        tmp_ap = self.aps.index(k) #隕九▽縺九▲縺溯｡後�ｮAP隕∫ｴ�逡ｪ蜿ｷ
                        #tmp_ap = k
                        break
                #print("AP縺ｮ隕∫ｴ�逡ｪ蜿ｷ:{}".format(time.time()-start))
                if(to_ap == -1 ): #譛�蛻昴�ｮ隕九▽縺九▲縺溯｡�
#                    print("First")
                    to_ap = tmp_ap
                    #https://qiita.com/Sasagawa0185/items/1185933dd0e560a26b07
                    from_time = dt.datetime.strptime(j.timestamp, '%Y-%m-%d %H:%M:%S') #datetime蝙九↓螟画鋤
                    to_time = dt.datetime.strptime(j.timestamp, '%Y-%m-%d %H:%M:%S')
                    #蛻ｰ逹�譎ゅ�ｮ逋ｻ骭ｲ(貊槫惠譎る俣縺ｯ-1)
                    #self.transitions[self.aps.index('out')][to_ap] += 1 #謗ｨ遘ｻ逋ｻ骭ｲ
                    transition_from.append(self.aps.index('out'))
                    #transition_from.append('out')
                    transition_to.append(to_ap)
                    duration.append(-1)
                elif(to_ap != -1) : #邯壹￠縺ｦ隕九▽縺九▲縺溷�ｴ蜷�
                    if(to_ap == tmp_ap): #蜷後§AP縺ｧ縺ｮ謗ｨ遘ｻ
                        to_time = dt.datetime.strptime(j.timestamp, '%Y-%m-%d %H:%M:%S')
                    else: #驕輔≧AP縺ｸ縺ｮ謗ｨ遘ｻ
                        transition_from.append(to_ap)
                        transition_to.append(tmp_ap)
                        to_time = dt.datetime.strptime(j.timestamp, '%Y-%m-%d %H:%M:%S') #谺｡縺ｮ迥ｶ諷区耳遘ｻ縺ｮ譎る俣縺ｾ縺ｧ縺�縺溘→縺吶ｋ(荳願ｨ俶､懆ｨ惹ｺ矩��)
                        delta = to_time - from_time
                        duration.append(delta.total_seconds())
                        #self.duration_ap.append(to_ap)
                        from_ap = to_ap
                        to_ap = tmp_ap
                        from_time = dt.datetime.strptime(j.timestamp, '%Y-%m-%d %H:%M:%S') #datetime蝙九↓螟画鋤

        #蜈ｨ繝ｭ繧ｰ繝√ぉ繝�繧ｯ邨ゆｺ�
 #       print("Finish")
        #self.transitions[to_ap][self.aps.index('out')] += 1 #謗ｨ遘ｻ逋ｻ骭ｲ
        transition_from.append(to_ap)
        transition_to.append(self.aps.index('out'))
        #transition_to.append('out')
        delta = to_time - from_time
        duration.append(delta.total_seconds())

        print(i)
        print("time:{}".format(time.time()-start))
        return transition_from, transition_to, duration


    def getTransition(self, df, df_unique):
        self.pdf = pd.DataFrame(df) #繝�繝ｼ繧ｿ繝輔Ξ繝ｼ繝�繧恥andas縺ｫ螟画鋤 (荳ｦ蛻苓ｨ育ｮ励�ｮ縺溘ａ繧､繝ｳ繧ｹ繧ｿ繝ｳ繧ｹ螟画焚縺ｫ螟画峩2020/03/25)
        # 20200329謌ｻ繧雁�､繧貞渚譏�縺輔○繧九ｄ繧頑婿縺ｧ蜀榊ｮ滓命
        # https://qiita.com/kokumura/items/2e3afc1034d5aa7c6012
        with ProcessPoolExecutor(max_workers= 3) as executor:
            futures = [executor.submit(self.doTask, i) for i in df_unique['client']]
            start = time.time()
            for future in concurrent.futures.as_completed(futures):
                transition_from, transition_to, duration = future.result()
                self.transition_from.extend(transition_from)
                self.transition_to.extend(transition_to)
                self.duration.extend(duration)
                self.roop_index += 1
                print( str(self.roop_index) + '/' + str(len(df_unique)))

                print("処理1:{}".format(time.time()-start))
        #譛�蠕後↓謗ｨ遘ｻ遒ｺ邇�陦悟�励�ｮ菴懈��
        for i, j in zip(self.transition_from, self.transition_to):
            self.transitions[i][j] += 1
        print("処理2:{}".format(time.time()-start))

    def getTransitionData(self):
        return self.transitions

    def saveCSV(self, df, fname):
        pdf = pd.DataFrame(df) #繝�繝ｼ繧ｿ繝輔Ξ繝ｼ繝�繧恥andas縺ｫ螟画鋤
        pdf.to_csv(fname)
