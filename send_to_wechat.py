# -*- coding: utf-8 -*-
import time
import requests
import json
import urllib.request
# import urllib
import os


class WeChat:
    def __init__(self):
        self.CORPID = 'wwd37079c917e06dbb'  #企业ID，在管理后台获取
        self.CORPSECRET = 'bUWm53U6lGRH_vpwCQK0-dHp8HbagFoTKIgKbCAciTo'  #自建应用的Secret，每个自建应用里都有单独的secret
        self.AGENTID = '1000007'  #应用ID，在后台应用中获取

    def _get_access_token(self):
        url = 'https://qyapi.weixin.qq.com/cgi-bin/gettoken'
        values = {'corpid': self.CORPID,
                  'corpsecret': self.CORPSECRET,
                  }
        req = requests.post(url, params=values)
        data = json.loads(req.text)
        return data["access_token"]

    def get_access_token(self):
        try:
            filepath = os.path.abspath(os.path.dirname(__file__))
            os.mkdir(filepath+'/tmp')
        except:
            pass
        
        try:
            with open(filepath+'/tmp/access_token.conf', 'r') as f:
                t, access_token = f.read().split()
        except:
            with open(filepath+'/tmp/access_token.conf', 'w') as f:
                access_token = self._get_access_token()
                cur_time = time.time()
                f.write('\t'.join([str(cur_time), access_token]))
                return access_token
        else:
            cur_time = time.time()
            if 0 < cur_time - float(t) < 7260:
                return access_token
            else:
                with open(filepath+'/tmp/access_token.conf', 'w') as f:
                    access_token = self._get_access_token()
                    f.write('\t'.join([str(cur_time), access_token]))
                    return access_token

    def send_data(self, message,touser='',toparty=''):
        send_url = 'https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=' + self.get_access_token()
        send_values = {
            "msgtype": "text",
            "agentid": self.AGENTID,
            "text": {
                "content": message
                },
            "safe": "0"
            }
        if len(touser)>0:
            send_values["touser"] = touser
        if len(toparty)>0:
            send_values["toparty"] = int(toparty)
        send_msges=(bytes(json.dumps(send_values), 'utf-8'))
        respone = requests.post(send_url, send_msges)
        respone = respone.json()   #当返回的数据是json串的时候直接用.json即可将respone转换成字典
        print(respone["errmsg"])
        return respone["errmsg"]

    def get_media_ID(self, path):  ##上传到临时素材  图片ID
        Gtoken = self.get_access_token()
        img_url = "https://qyapi.weixin.qq.com/cgi-bin/media/upload?access_token={}&type=image".format(Gtoken)
        xx = {'type':'image', 'offset':0, 'count':20}
        files = {'image': open(path, 'rb')}
        r = requests.post(img_url, json=json.dumps(xx),files=files)
        print(r.text)
        re = json.loads(r.text)
        return re['media_id']

    def send_pic(self, path, touser='', toparty=''):  ##发送图片
        img_id = self.get_media_ID(path)
        post_data1 = {}
        msg_content1 = {}
        msg_content1['media_id'] = img_id
        if len(touser)>0:
            post_data1['touser'] = touser
        if len(toparty)>0:
            post_data1['toparty'] = int(toparty)
        post_data1['msgtype'] = 'image'
        post_data1['agentid'] = self.AGENTID
        post_data1['image'] = msg_content1
        post_data1['safe'] = '0'
        Gtoken = self.get_access_token()
        purl2="https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={}".format(Gtoken)
        json_post_data1 = json.dumps(post_data1)
        request_post = urllib.request.urlopen(purl2,json_post_data1.encode(encoding='UTF8'))
        return request_post


if __name__ == '__main__':
    wx = WeChat()
    wx.send_data("test!", touser='hujinglei')
    # wx.send_pic("c:/Users/Admin/Pictures/xy.jpg")
    # wx.send_data("这是程序发送的第2条消息！")
