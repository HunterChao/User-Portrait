        create table working.keep_zhaoht_peasona_user_tag_sns_01
		as
		select user_id,
			   org_id,
			   org_name,
			   cnt,
			   date_id,
			   tag_type_id,
			   act_type_id
		  from dw.peasona_user_tag_relation  --用户标签表
		 where date_id >='2017-01-01'
		   and date_id <='2017-07-31'
		   and tag_type_id=12		
		   and act_type_id in (14,15,43,44,45)
	  group by user_id,
			   org_id,
			   org_name,
			   cnt,
			   date_id,
			   tag_type_id,
			   act_type_id
			   
-- 计算每个标签 及标签对应的用户人数
		create table working.keep_zhaoht_peasona_user_tag_sns_02
		as
		select org_id,
			   org_name,
			   count(distinct user_id) user_num,
			   row_number() over (order by count(distinct user_id) desc) rank
		  from working.keep_zhaoht_peasona_user_tag_sns_01
	  group by org_id,
			   org_name
			   
	
	
-- 计算每两个标签共同关注人数 同现矩阵
		 create table working.keep_zhaoht_peasona_user_tag_sns_03
		 as
		 select t.org_id_1,
			    t.org_name_1,
				t.org_id_2,
				t.org_name_2,
				num
		   from (
		        select t1.org_id org_id_1,
				       t1.org_name org_name_1,
					   t2.org_id org_id_2,
				       t2.org_name org_name_2,
					   count(distinct t1.user_id) as num
				  from working.keep_zhaoht_peasona_user_tag_sns_01 t1
			inner join working.keep_zhaoht_peasona_user_tag_sns_01 t2
			        on t1.user_id = t2.user_id		--同一个标签
				 where t1.org_id <> t2.org_id		--不同的用户
			  group by t1.org_id,
				       t1.org_name,
					   t2.org_id,
					   t2.org_name
			      ) t
				  
		 
		 
 -- 余弦相似度矩阵 计算两个标签间相关性
         create table working.keep_zhaoht_peasona_user_tag_sns_04
		 as
		 select t1.org_id_1,
				t1.org_name_1,
				t2.user_num_1,
				t1.org_id_2,
				t1.org_name_2,
				t3.user_num_2,
				t1.num,
				(t1.num/sqrt(t2.user_num_1 * t3.user_num_2)) as power,
				row_number() over(order by (t1.num/sqrt(t2.user_num_1 * t3.user_num_2)) desc) rank
		   from working.keep_zhaoht_peasona_user_tag_sns_03 t1
	  left join (
				select org_id,
				       user_num as user_num_1
				  from working.keep_zhaoht_peasona_user_tag_sns_02 
				  ) t2
			 on t1.org_id_1 = t2.org_id
	  left join (
				select org_id,
				       user_num as user_num_2
				  from working.keep_zhaoht_peasona_user_tag_sns_02 
				  ) t3
			 on t1.org_id_2 = t3.org_id
		group by t1.org_id_1,
				 t1.org_name_1,
				 t2.user_num_1,
				 t1.org_id_2,
				 t1.org_name_2,
				 t3.user_num_2,
				 t1.num,
		         (t1.num/sqrt(t2.user_num_1 * t3.user_num_2))
	
	
-- 抽取标签，看一下效果		
	
	select  *  from  working.keep_zhaoht_peasona_user_tag_sns_04   
		where user_num_1 > 20
		and user_num_2 > 20
		and power>0.7
		order by rank
		 limit 1000
		 
		 
-- 用户的帖子标签偏好度

          create table working.keep_zhaoht_peasona_user_tag_sns_05
		  as
		  select t1.user_id,
				 t1.org_id,
				 t1.org_name,
				 case when t2.is_time_reduce = 1 then 
				      exp(cast(datediff('2017-08-01' , t1.date_id) as int) * (-0.00770164)) * cast(t2.act_weight as float) * cast(t1.cnt as int)
					  when t2.is_time_reduce = 0 then 
					  cast(t2.act_weight as float) *  cast(t1.cnt as int)
				 end as act_weight,
				 t1.cnt
			from working.keep_zhaoht_peasona_user_tag_sns_01 t1
	  inner join dim.peasona_user_act_weight_manual t2
			  on t1.act_type_id = t2.act_type_id
		   where t2.plan_id = 3
		     and t1.tag_type_id in (12)
				 
--看自己标签一下效果：select * from  working.keep_zhaoht_peasona_user_tag_sns_05 where user_id = '45709136' 

-- 对每个用户的历史帖子标签做权重加总
         create table working.keep_zhaoht_peasona_user_tag_sns_06
		 as
		 select user_id,
			    org_id,
			    org_name,
			    sum(act_weight) as weight,
				row_number() over(order by sum(act_weight) desc) as rank
		   from working.keep_zhaoht_peasona_user_tag_sns_05
	   group by user_id,
			    org_id,
			    org_name
				
		
		 
		 
		 
	-- 用户评分矩阵 ： 	working.keep_zhaoht_peasona_user_tag_sns_06
	-- 帖子相互关系同现矩阵：  working.keep_zhaoht_peasona_user_tag_sns_04
	-- 推荐用户的标签 = 帖子相互关系同现矩阵 * 用户评分矩阵
		 
		 
	 --working.keep_zhaoht_peasona_user_tag_sns_08      user_num_1 > 10000  标签57种
	 --working.keep_zhaoht_peasona_user_tag_sns_09      user_num_1 > 500  标签354种    
	 --working.keep_zhaoht_peasona_user_tag_sns_07      user_num_1 > 1000  标签230种 
	 
	 -- 计算推荐给用户的医言堂相关标签
		 create table working.keep_zhaoht_peasona_user_tag_sns_09
		 as
		 select user_id,
				org_id_2,
				org_name_2,
		        recommend
		   from (
				 select user_id,
						org_id_2,
						org_name_2,
						recommend,
						row_number() over(partition by user_id order by recommend desc) row_rank
				   from (				
						 select user_id,
								org_id_2,
								org_name_2,
								recommend
						   from(
								select t2.user_id,
									   t1.org_id_2,
									   t1.org_name_2,
									   sum(t1.power * t2.weight) as recommend,
									   row_number() over (order by sum(t1.power * t2.weight) desc) as rank
								  from working.keep_zhaoht_peasona_user_tag_sns_04 t1
							inner join working.keep_zhaoht_peasona_user_tag_sns_06 t2
									on t1.org_id_1 = t2.org_id
								 where t1.user_num_1 > 500		--取>500人同时存在的标签
								   and t1.user_num_2 > 500
							  group by t2.user_id,
									   t1.org_id_2,
									   t1.org_name_2
								) t
							) t1
					) t1
					where row_rank <=20		--每个用户取前20个标签

					
		 
		 --- 相关博客   http://www.2cto.com/kf/201607/522842.html               
		 ---            http://www.procedurego.com/article/147603.html
		 
		 
		 
		 
		 
	