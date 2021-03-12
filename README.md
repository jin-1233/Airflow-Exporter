# Airflow-Exporter

#### 一个关于airflow指标的自定义exporter

#### config.yml

```
改配置文件中需要你去设置需要访问的airflow的ip地址
nginx_ip：能访问airflow的地址（如果没有转发，直接写airflow的真实地址）
remote_ip: 真实的airflow地址（指标中确定airflow属于哪一台机器）
```

### start

```
python3 Jin_Exporter.py
```



目前只有失败的task的指标，如果有其他的需要可自行在此基础拓展，若有疑问，可添加作者本人进行交流：1259185603@qq.com 谢谢！

提示：作者为新手小白，希望各路大神多多指点，谢谢



