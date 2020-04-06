/*drop table if exists dw.user_info_whole_df;
create table dw.user_info_whole_df (
       user_id                bigint     comment '用户编码'
     , login_name             string     comment '登录名称'
     , user_name              string     comment '用户姓名'
     , user_status_id         int        comment '用户状态: 0未激活, 1已激活, 2作废, 3黄牛禁用'
     , gender_id              int        comment '用户性别: 1男, 2女, 3未知'
     , birthday               int        comment '用户生日'
     , user_age               int        comment '用户年龄'
     , constellation_name     string     comment '用户星座名称: 白羊座(03.21-04.19), 金牛座(04.20-05.20), 双子座(05.21-06.21), 巨蟹座(06.22-07.22), 狮子座(07.23-08.22), 处女座(08.23-09.22), 天秤座(09.23-10.23), 天蝎座(10.24-11.22), 射手座(11.23-12.21), 魔羯座(12.22-01.19), 水瓶座(01.20-02.18), 双鱼座(02.19-03.20)'
     , zodiac_name            string     comment '用户生肖名称: 鼠, 牛, 虎, 兔, 龙, 蛇, 马, 羊, 猴, 鸡, 狗, 猪'
     , cellphone_id           string     comment '用户手机编码'
     , cert_id                string     comment '用户证件号码'
     , source_id              bigint     comment '用户注册来源: 代码表dim.source_info'
     , sep_sources_id         bigint     comment '用户注册分离来源: 代码表dim.user_sep_source'
     , source_channle         bigint     comment '第三方应用编码'
     , cert_std_region_id     string     comment '证件归属地标准区域编码'
     , cert_region_name       string     comment '证件归属地区域名称'
     , cert_std_zone_id       string     comment '证件归属地标准战区编码'
     , cert_zone_name         string     comment '证件归属地战区名称'
     , cert_std_province_id   string     comment '证件归属地标准省份编码'
     , cert_province_id       string     comment '证件归属地省份编码'
     , cert_province_name     string     comment '证件归属地省份名称'
     , cert_std_city_id       string     comment '证件归属地标准市级编码'
     , cert_city_id           string     comment '证件归属地市级编码'
     , cert_city_name         string     comment '证件归属地市级名称'
     , cert_std_district_id   string     comment '证件归属地标准地市编码'
     , cert_district_id       string     comment '证件归属地地市编码'
     , cert_district_name     string     comment '证件归属地地市名称'
     , phone_std_region_id    string     comment '手机归属地标准区域编码'
     , phone_region_name      string     comment '手机归属地区域名称'
     , phone_std_zone_id      string     comment '手机归属地标准战区编码'
     , phone_zone_name        string     comment '手机归属地战区名称'
     , phone_std_province_id  string     comment '手机归属地标准省份编码'
     , phone_province_name    string     comment '手机归属地省份名称'
     , phone_std_city_id      string     comment '手机归属地标准市级编码'
     , phone_city_name        string     comment '手机归属地市级名称'
     , is_real_name_auth      int        comment '是否实名认证标志: 1强实名:健康账户认证, 2弱实名:保险等业务填写信息, 3一般实名:用户注册填写信息(必填的只有手机号,选填有姓名,身份证号,位置信息等), 4未实名:未填写身份证信息'
     , is_valid_cellphone     int        comment '是否认证手机标志: 1是, 0否'
     , is_has_photo           int        comment '是否有头像标志: 1是, 0否'
     , is_tmp_user_flag       int        comment '是否临时用户标志: 1是, 0否'
     , create_time            timestamp  comment '注册时间'
     , create_date            string     comment '注册日期'
     , modify_time            timestamp  comment '修改时间'
     , modify_date            string     comment '修改日期'
     , date_id                string     comment '数据日期'
     , user_type              int        comment '用户类型, 0:患者, 1:医生'  --20170720 add
     , is_health_user         int        comment '是否开通健康号'
     , reg_type_id            tinyint    comment '第三方注册渠道'
     , service_provider       string     comment '手机所属运营商'
     , phone_city_code        string     comment '手机所属城市区号'
     , phone_post_code        string     comment '手机所属城市邮编'
)
row format delimited
fields terminated by '\t'
lines terminated by '\n'
stored as textfile
;*/

       /*买过保险的用户*/
       drop table if exists tmp.t_t_user_info_whole_df_01;
       create table tmp.t_t_user_info_whole_df_01
       as
       select distinct user_id
         from (

               select holder_user_id  as user_id
                 from dwd.insurance_df
                where date_id = '${HIVE_DATA_DATE}'
                  and holder_user_id is not null
            union all
               select insured_user_id  as user_id
                 from dwd.insurance_df
                where date_id = '${HIVE_DATA_DATE}'
                  and insured_user_id is not null

               ) t1
            ;

       /*个人账户强实名的用户*/
       drop table if exists tmp.t_t_user_info_whole_df_02;
       create table tmp.t_t_user_info_whole_df_02
       as
       select cast(t1.user_id as bigint)  as user_id
         from dwd.t_urm_bind_dz t1  --三方与支付平台用户关系绑定表
    left join dwd.t_urm_pinf_di t2  --手机用户信息表
           on t1.payadm_user_no = t2.payadm_user_no
        where t1.date_id = '${HIVE_DATA_DATE}'
          and t1.link_start_date <= '${HIVE_DATA_DATE}'
          and t1.link_end_date>= '${HIVE_DATA_DATE}'
          and t1.bind_status = '0'  --0:已绑定
          and t2.date_id = '${HIVE_DATA_DATE}'
          and t2.real_name_flag = '02'  --02:强实名
            ;

       /*注册信息*/
       drop table if exists tmp.t_t_user_info_whole_df_03;
       create table tmp.t_t_user_info_whole_df_03
       as
       select profile.user_id  as user_id
            , users.login_id  as login_name
            , profile.user_name  as user_name
            , users.status_id  as user_status_id
            , case when profile.cert_sex = 1 then  --1:男
                        1  --男
                   when profile.cert_sex = 0 then  --0:女
                        2  --女
                   when profile.gender_id = 1 then  --1:男
                        1  --男
                   when profile.gender_id = 2 then  --2:女
                        2  --女
                   else -99  --未知
              end  as gender_id
            , profile.cert_birthday  as birthday
            , case when cast(substr(profile.cert_birthday, 1, 4) as int) = 1700 then
                        null
                   else cast(substr('${HIVE_DATA_DATE}', 1, 4) as int) - cast(substr(profile.cert_birthday, 1, 4) as int)
               end  as user_age  --如果生日是1700, 那么对应的年龄就是-99
            , case when profile.cert_birthday = '17000101' then '其他'
                   when substr(profile.cert_birthday, 5, 4) between '0321' and '0419' then '白羊座'
                   when substr(profile.cert_birthday, 5, 4) between '0420' and '0520' then '金牛座'
                   when substr(profile.cert_birthday, 5, 4) between '0521' and '0621' then '双子座'
                   when substr(profile.cert_birthday, 5, 4) between '0622' and '0722' then '巨蟹座'
                   when substr(profile.cert_birthday, 5, 4) between '0723' and '0822' then '狮子座'
                   when substr(profile.cert_birthday, 5, 4) between '0823' and '0922' then '处女座'
                   when substr(profile.cert_birthday, 5, 4) between '0923' and '1023' then '天秤座'
                   when substr(profile.cert_birthday, 5, 4) between '1024' and '1122' then '天蝎座'
                   when substr(profile.cert_birthday, 5, 4) between '1123' and '1221' then '射手座'
                   when substr(profile.cert_birthday, 5, 4) between '1222' and '1231' then '魔羯座'
                   when substr(profile.cert_birthday, 5, 4) between '0101' and '0119' then '魔羯座'
                   when substr(profile.cert_birthday, 5, 4) between '0120' and '0218' then '水瓶座'
                   when substr(profile.cert_birthday, 5, 4) between '0219' and '0320' then '双鱼座'
                   else '其他'
               end  as constellation_id
            , case when profile.cert_birthday = '17000101' then '其他'
                   when (abs(cast(substr(profile.cert_birthday, 1, 4) as int)-4)%12) + 1 = 1 then '鼠'
                   when (abs(cast(substr(profile.cert_birthday, 1, 4) as int)-4)%12) + 1 = 2 then '牛'
                   when (abs(cast(substr(profile.cert_birthday, 1, 4) as int)-4)%12) + 1 = 3 then '虎'
                   when (abs(cast(substr(profile.cert_birthday, 1, 4) as int)-4)%12) + 1 = 4 then '兔'
                   when (abs(cast(substr(profile.cert_birthday, 1, 4) as int)-4)%12) + 1 = 5 then '龙'
                   when (abs(cast(substr(profile.cert_birthday, 1, 4) as int)-4)%12) + 1 = 6 then '蛇'
                   when (abs(cast(substr(profile.cert_birthday, 1, 4) as int)-4)%12) + 1 = 7 then '马'
                   when (abs(cast(substr(profile.cert_birthday, 1, 4) as int)-4)%12) + 1 = 8 then '羊'
                   when (abs(cast(substr(profile.cert_birthday, 1, 4) as int)-4)%12) + 1 = 9 then '猴'
                   when (abs(cast(substr(profile.cert_birthday, 1, 4) as int)-4)%12) + 1 = 10 then '鸡'
                   when (abs(cast(substr(profile.cert_birthday, 1, 4) as int)-4)%12) + 1 = 11 then '狗'
                   when (abs(cast(substr(profile.cert_birthday, 1, 4) as int)-4)%12) + 1 = 12 then '猪'
                   else '其他'
               end  as zodiac_id
            , profile.mobile  as cellphone_id
            , profile.cert_no  as cert_id
            , profile.source_id  as source_id
            , users.sep_source_id  as seq_source_id
            , profile.source_channle_id  as source_channle
            , profile.is_valid_mobile  as is_valid_cellphone
            , case when profile.img_url is not null then
                        1
                   else 0
               end  as is_has_photo
            , users.reg_type_id is_tmp_user_flag
            , profile.cert_region_id
            , profile.cellphone_region_id
            , profile.create_time  as gmt_created
            , profile.create_date  as gmt_created_date
            , profile.modify_time  as gmt_modified
            , profile.modify_time  as gmt_modified_date
            , users.user_type_id  as user_type
            , users.reg_type_id  as reg_type_id
         from dwd.user_profile_dz profile
   inner join dwd.users_dz users
           on profile.user_id = users.user_id
        where profile.date_id = '${HIVE_DATA_DATE}'
          and profile.link_start_date <= '${HIVE_DATA_DATE}'
          and profile.link_end_date >= '${HIVE_DATA_DATE}'
          and users.date_id = '${HIVE_DATA_DATE}'
          and users.link_start_date <= '${HIVE_DATA_DATE}'
          and users.link_end_date >= '${HIVE_DATA_DATE}'
            ;

       drop table if exists tmp.t_t_user_info_whole_df_04;
       create table tmp.t_t_user_info_whole_df_04
       as
       select region_id
            , region_name
            , zone_id
            , zone_name
            , province_id
            , cert_province_id
            , province_name
            , city_id
            , cert_city_id
            , city_name
            , district_id
            , cert_district_id
            , district_name
         from (
               select region_id
                    , region_name
                    , zone_id
                    , zone_name
                    , province_id
                    , cert_province_id
                    , province_name
                    , city_id
                    , cert_city_id
                    , city_name
                    , district_id
                    , cert_district_id
                    , district_name
                    , row_number() over(partition by cert_district_id order by district_id) row_id
                 from dim.cert_province_city_district
               ) t1
        where row_id = 1
            ;

       /*插入目标表*/
       insert overwrite table dw.user_info_whole_df
       select t1.user_id
            , t1.login_name
            , t1.user_name
            , t1.user_status_id
            , t1.gender_id
            , t1.birthday
            , t1.user_age
            , t1.constellation_id
            , t1.zodiac_id
            , t1.cellphone_id
            , t1.cert_id
            , t1.source_id
            , t1.seq_source_id
            , t1.source_channle
            , case when d.region_id is not null then
                        d.region_id
                   else '-99'
               end  as cert_std_region_id
            , case when d.region_name is not null then
                        d.region_name
                   else '-99'
               end  as cert_region_name

            , case when d.zone_id is not null then
                        d.zone_id
                   else '-99'
               end  as cert_std_zone_id
            , case when d.zone_name is not null then
                        d.zone_name
                   else '-99'
               end  as cert_zone_name

            , case when d.province_id is not null then
                        d.province_id
                   else '-99'
               end  as cert_std_province_id
            , case when d.cert_province_id is not null then
                        d.cert_province_id
                   else '-99'
               end  as cert_province_id
            , case when d.province_name is not null then
                        d.province_name
                   else '-99'
               end  as cert_province_name

            , case when d.city_id is not null then
                        d.city_id
                   else '-99'
               end  as cert_std_city_id
            , case when d.cert_city_id is not null then
                        d.cert_city_id
                   else '-99'
               end  as cert_city_id
            , case when d.city_name is not null then
                        d.city_name
                   else '-99'
               end  as cert_city_name

            , case when d.district_id is not null then
                        d.district_id
                   else '-99'
               end  as cert_std_district_id
            , case when d.cert_district_id is not null then
                        d.cert_district_id
                   else '-99'
               end  as cert_district_id
            , case when d.district_name is not null then
                        d.district_name
                   else '-99'
               end  as cert_district_name
            , case when phone.region_id is not null then
                        phone.region_id
                   else '-99'
               end  as phone_std_region_id
            , case when phone.region_name is not null then
                        phone.region_name
                   else '-99'
               end  as phone_region_name
            , case when phone.zone_id is not null then
                        phone.zone_id
                   else '-99'
               end  as phone_std_zone_id
            , case when phone.zone_name is not null then
                        phone.zone_name
                   else '-99'
               end  as phone_zone_name
            , case when phone.province_id is not null then
                        phone.province_id
                   else '-99'
               end  as phone_std_province_id
            , case when phone.province_name is not null then
                        phone.province_name
                   else '-99'
               end  as phone_province_name
            , case when phone.city_id is not null then
                        phone.city_id
                   else '-99'
               end  as phone_std_city_id
            , case when phone.city_name is not null then
                        phone.city_name
                   else '-99'
               end  as phone_city_name
            , case when t2.user_id is not null then
                        1  --健康账户强实名
                   when t3.user_id is not null then
                        2  --买过保险的
                   when t1.cellphone_id is not null
                    and t1.cert_id is not null then
                        3  --填写过手机和身份证的
                   else 4  --手机或者身份证不全的
               end  as is_real_name_auth
            , t1.is_valid_cellphone
            , t1.is_has_photo
            , t1.is_tmp_user_flag
            , t1.gmt_created
            , t1.gmt_created_date
            , t1.gmt_modified
            , t1.gmt_modified_date
            , '${HIVE_DATA_DATE}'  as date_id
            , t1.user_type
            , case when c.user_id is not null then 1
                else 0
              end  as is_health_user
            , t1.reg_type_id
            , case when phone.service_provider is not null then
                   phone.service_provider
                else '其他'
                end  as service_provider
            , case when phone.city_code  is not null then
                 phone.city_code
                else '-99'
                 end  as phone_city_code
            , case when phone.post_code is not null then 
                 phone.post_code
                else '-99'
                  end as phone_post_code
         from tmp.t_t_user_info_whole_df_03 t1
    left join tmp.t_t_user_info_whole_df_02 t2  --健康账户临时表
           on t1.user_id = t2.user_id
    left join tmp.t_t_user_info_whole_df_01 t3  --保险临时表
           on t1.user_id = t3.user_id
    left join tmp.t_t_user_info_whole_df_04 d  --证件归属地代码表
           on t1.cert_region_id = d.cert_district_id
    left join dim.phone_province_city_manual phone  --手机号码归属地代码表
           on case when t1.cellphone_region_id = '0000000' then concat('hive',rand()) else t1.cellphone_region_id end = phone.phone_zone_id
    left join dwd.cloud_sns_health_df c
           on t1.user_id = c.user_id
          and c.date_id='${PART_DATE}'
          and c.create_date <= '${HIVE_DATA_DATE}'
          and c.is_deleted = '0'
            ;

   drop table if exists tmp.t_t_user_info_whole_df_01;
   drop table if exists tmp.t_t_user_info_whole_df_02;
   drop table if exists tmp.t_t_user_info_whole_df_03;
   drop table if exists tmp.t_t_user_info_whole_df_04;