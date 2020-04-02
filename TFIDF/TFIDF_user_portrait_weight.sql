
-- TF计算每个人身上的标签权重
		drop table if exists temp.keep_zhaoht_user_tfidf_01;
		create table temp.keep_zhaoht_user_tfidf_01
		as 
		select tt1.user_id,
			   tt1.org_id,
			   tt1.org_name,
			   tt1.weight_m_p,
			   tt2.weight_m_s
		from (
			 select t1.user_id,
					t1.org_id,
					t1.org_name,
					count(t1.org_id) weight_m_p  --每个人每类标签个数
			   from dw.peasona_user_tag_relation t1
		   group by 1,2,3
			 ) tt1
	left join (
			  select t2.user_id,
					 count(t2.org_id) weight_m_s	--每个人身上标签总数
				from dw.peasona_user_tag_relation t2
			group by 1
			 ) tt2
		  on tt1.user_id = tt2.user_id 
					
					
-- IDF计算每个标签在全体标签中的权重
		drop table if exists temp.keep_zhaoht_user_tfidf_02;
		create table temp.keep_zhaoht_user_tfidf_02
		as 
		select tt1.org_id,
			   tt1.org_name,
			   tt1.weight_w_p,
			   tt2.weight_w_s
		 from (	select t1.org_id,
					   t1.org_name,
					   cast(sum(weight_m_p) as int) as weight_w_p	--每个标签一共有多少
				  from temp.keep_zhaoht_user_tfidf_01 t1
			  group by 1,2 
			  ) tt1
   cross join (select sum(t2.weight_m_p) as weight_w_s		--全体所有标签的总个数
				  from temp.keep_zhaoht_user_tfidf_01 t2
			   ) tt2
					
					
-- TF-IDF计算每个人身上标签权重 (到这个表为止，用户身上每个标签的TFIDF权重计算完)
	  drop table if exists temp.keep_zhaoht_user_tfidf_03;
	  create table temp.keep_zhaoht_user_tfidf_03
	  as 
	  select t1.user_id,
			 t1.org_id,
			 t1.org_name,
			 --t1.weight_m_p,
			 --t1.weight_m_s,
			 --t2.weight_m_p,
			 --t2.weight_m_s,
			 --t3.weight_w_p,
			 --t3.weight_w_s,
			 (t1.weight_m_p/t1.weight_m_s)*(log10(t2.weight_w_s/t2.weight_w_p)) as ratio   --TFIDF计算每个用户每个标签权重
		from temp.keep_zhaoht_user_tfidf_01 t1     -- 用户标签表
	left join temp.keep_zhaoht_user_tfidf_02 t2
	       on t1.org_id = t2.org_id
	
					
					
					
					
-- 用户标签权重计算 = 行为时间衰减 * 行为权重 * 行为次数 * 标签TFIDF	
	create table temp.keep_zhaoht_user_tfidf_04
	as
	select tt1.user_id,
		   tt1.org_id,
		   tt1.org_name,
		   tt1.cnt,		--行为次数
		   tt1.date_id,
		   tt1.act_type_id,	--行为类型
		   tt1.tag_type_id,		--标签类型
		   tt1.act_weight		--标签权重
	from (
		 select t1.user_id,
				t1.org_id,
				t1.org_name,
				t1.cnt,
				t1.date_id,
				t1.act_type_id,
				t1.tag_type_id,
				case when t2.is_time_reduce = 1 then
					 exp(datediff('2017-07-20' , t1.date_id) * (-0.00770164)) * cast(t2.act_weight as float) * cast(t1.cnt as int) * t3.ratio
					 when t2.is_time_reduce = 0 then
					 cast(t2.act_weight as float) * cast(t1.cnt as int) * t3.ratio
				end as act_weight
		 from dw.peasona_user_tag_relation t1
	left join dim.peasona_user_act_weight_manual t2
		   on t1.act_type_id = t2.act_type_id
	left join temp.keep_zhaoht_user_tfidf_03 t3
		   on t1.user_id = t3.user_id and t1.org_id = t3.org_id
		where t1.date_id >= DATE_SUB('2017-07-20', 365)
		  and t2.plan_id = 1 
		 ) tt1
	where tt1.user_id is not null
						
					
				 
	-- 查询某个用户的标签：用新建的TFIDF综合权重计算			
	  select user_id,
			 org_id,
			 org_name,
			 sum(act_weight) as weight
	   from temp.keep_zhaoht_user_tfidf_04
	  where user_id='45709136'
   group by 1,2,3
   order by weight desc 
					
	
	-- 查询某个用户的标签 ：用user_medical_tag_weight_portrait计算
	select user_id,
           portrait_id,
           portrait_name,
           sum(weight_order) as weight
      from gdw_dm.user_medical_tag_weight_portrait  
     where user_id=45709136
  group by 1,2,3
  order by weight desc 
					
					
	45192790
					
					
					
					
					
					
					
					