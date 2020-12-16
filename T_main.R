T_run = function(dbname){
  t = Sys.time()
  gc()
  # 读取temp_comment
  m = mongo(collection = 'temp_comment',db = dbname)
  temp_comment = m$find()
  
  # 筛选近30天、近30~60天、60~20160101、90
  lasttime = max(temp_comment$Date) %>% as.Date(., tz = 'ASIA/SHANHAI')
  index1 = temp_comment$Date >= (lasttime - 29)
  index2 = (temp_comment$Date >= (lasttime - 59) & temp_comment$Date < (lasttime - 29))
  index3 = !(index1 | index2)
  index4 = (temp_comment$Date >= (lasttime - 89) & temp_comment$Date < (lasttime - 59))
  temp_comment30 = temp_comment[index1, ]
  temp_comment60 = temp_comment[index2, ]
  temp_commentElse = temp_comment[index3, ]
  temp_comment90 = temp_comment[index4, ]
  
  # log
  logReset()
  filename <- paste0(filepath, "/log/",Sys.Date(), dbname, "_T_main.log")
  basicConfig(level = 'FINEST')
  addHandler(writeToFile, file = filename, level = 'DEBUG')
  # with(getLogger(), names(handlers))
  
  # start
  # 评论次数趋势图, qsc趋势图, 负面情绪趋势图
  tryCatch({
    ln <- 'T_allinf'
    T_allinf = all_inf_fun(temp_comment, type = "month")

    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 一个账号的留言次数
  tryCatch({
    ln <- 'T_userinf'
    T_userinf = user_inf_fun(temp_comment)
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 文字探勘----
  # 全词云
  tryCatch({
    ln <- 'T_wcall'
    # 近30
    T_wcall = word_cloud_fun(temp_comment30$Comment, 10)
    if(nrow(T_wcall) > 50) T_wcall %<>% .[1:50, ]
    colnames(T_wcall) = c('name', 'value')
    wc_word = filter_segment(T_wcall$name, filter_word) %>% data.frame(name = ., stringsAsFactors = F)
    T_wcall = inner_join(T_wcall, wc_word, by = 'name')
    T_wcall$ID = seq(1, nrow(T_wcall)); T_wcall %<>% select(., ID, name, value)
    
    # 30~60
    T_wcall60 = word_cloud_fun(temp_comment60$Comment, 10)
    if(nrow(T_wcall60) > 50) T_wcall60 %<>% .[1:50, ]
    colnames(T_wcall60) = c('name', 'value')
    wc_word = filter_segment(T_wcall60$name, filter_word) %>% data.frame(name = ., stringsAsFactors = F)
    T_wcall60 = inner_join(T_wcall60, wc_word, by = 'name')
    T_wcall60$ID = seq(1, nrow(T_wcall60))
    T_wcall60 %<>% select(., ID, name, value)
    
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # QSC特殊字
  tryCatch({
    ln <- 'qscWords'
    # 近30
    qscWords = qscWords_fun(temp_comment30)
    T_labelword = qsctolabel_fun(qscWords)
    T_labelword = inner_join(T_labelword[, -2], lable_select, by = c('term')) %>% select(., term, score, ID)
    
    # 30~60
    qscWords = qscWords_fun(temp_comment60)
    T_labelword60 = qsctolabel_fun(qscWords)
    T_labelword60 = inner_join(T_labelword60[, -2], lable_select, by = c('term')) %>% select(., term, score, ID)
    
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 上升词,下降词
  tryCatch({
    ln <- 'allrank'
    
    # 近30
    allrank = wordChange_fun(temp_comment, TD = T, type = 'month', now = temp_comment30, last = temp_comment)
    allrank = allrank[,c(1,6)]; colnames(allrank) = c('name', 'value')
    # 新鲜
    T_upword = arrange(allrank, desc(value))
    if(nrow(T_upword)) T_upword %<>% .[1:20, ]
    # 过时
    T_downword = arrange(allrank, value)
    if(nrow(T_downword)) T_downword %<>% .[1:20, ]
    
    # 30~60
    allrank = wordChange_fun(temp_comment, TD = T, type = 'month', now = temp_comment60, last = rbind(temp_commentElse, temp_comment60))
    allrank = allrank[,c(1,6)]
    colnames(allrank) = c('name', 'value')
    # 新鲜
    T_upword60 = arrange(allrank, desc(value))
    if(nrow(T_upword60)) T_upword60 %<>% .[1:20, ]
    # 过时
    T_downword60 = arrange(allrank, value)
    if(nrow(T_downword60)) T_downword60 %<>% .[1:20, ]
    
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 预处理的dtm，给关联字使用
  tryCatch({
    ln <- 'T_asodtm'
    # 近30
    asodtm = asodtm_fun(temp_comment30)
    # 30~60
    asodtm60 = asodtm_fun(temp_comment60)
    
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 7 新鲜字(上升最多的字词)
  tryCatch({
    ln <- 'T_upword'
    # 近30
    sevenall = T_upword_f(T_upword, 'month')
    T_upword %<>% zoomin_allword(temp_comment30, ., asodtm)
    
    # 30~60
    sevenall60 = T_upword_f(T_upword60, 'month')
    T_upword60 %<>% zoomin_allword(temp_comment60, ., asodtm60)
    
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 8 过时字(下降最多的字词)
  tryCatch({
    ln <- 'T_downword'
    # 近30
    eightall = T_downword_f(T_downword, 'month')
    T_downword$value %<>% abs(.)
    T_downword %<>% zoomin_allword(temp_comment30, ., asodtm)
    
    # 30~60
    eightall60 = T_downword_f(T_downword60, 'month')
    T_downword60$value %<>% abs(.)
    T_downword60 %<>% zoomin_allword(temp_comment60, ., asodtm60)
    
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  # ----
  
  # 门店----
  tryCatch({
    ln <- 'shopinf'
    # 近30
    shopinf = shop_inf_fun(temp_comment30)
    shopinf$qsc = (shopinf$taste + shopinf$envir + shopinf$service) / 3
    shopinf = na.omit(shopinf)
    T_shopinf = shopinf
    
    # 30~60
    shopinf60 = shop_inf_fun(temp_comment60)
    shopinf60$qsc = (shopinf60$taste + shopinf60$envir + shopinf60$service) / 3
    shopinf60 = na.omit(shopinf60)
    T_shopinf60 = shopinf60
    
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 门店次数
  tryCatch({
    ln <- 'T_freqshop'
    # 近30
    temp = arrange(shopinf, desc(count))
    if(nrow(temp) > 20) temp = temp[1:20, ]
    T_freqshop = zoomin_spword_fun(temp_comment30, temp, type = 'shop', neg = F, asodtm)
    
    # 30~60
    temp = arrange(shopinf60, desc(count))
    if(nrow(temp) > 20) temp = temp[1:20, ]
    T_freqshop60 = zoomin_spword_fun(temp_comment60, temp, type = 'shop', neg = F, asodtm60)
    
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 门店分数
  tryCatch({
    ln <- 'T_scoreshop'
    # 近30
    temp = arrange(shopinf, qsc)[, c(1, 7)]
    if(nrow(temp) > 20) temp = temp[1:20, ]
    T_scoreshop = zoomin_spword_fun(temp_comment30, temp, type = 'shop', neg = T, asodtm)
    
    # 30~60
    temp = arrange(shopinf60, qsc)[, c(1, 7)]
    if(nrow(temp) > 20) temp = temp[1:20, ]
    T_scoreshop60 = zoomin_spword_fun(temp_comment60, temp, type = 'shop', neg = T, asodtm60)
    
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 门店情绪
  tryCatch({
    ln <- 'T_sentishop'
    # 近30
    temp = arrange(shopinf, desc(senti))[, c(1, 6)]
    if(nrow(temp) > 20) temp = temp[1:20, ]
    T_sentishop = zoomin_spword_fun(temp_comment30, temp, 'shop', neg = T, asodtm)
    
    # 30~60
    temp = arrange(shopinf60, desc(senti))[, c(1, 6)]
    if(nrow(temp) > 20) temp = temp[1:20, ]
    T_sentishop60 = zoomin_spword_fun(temp_comment60, temp, 'shop', neg = T, asodtm60)
    
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 门店榜
  tryCatch({
    ln <- 'T_changeshop'
    # 近30
    T_changeshop = heroboardForT_fun(temp_comment30, temp_comment60)
    
    # 进步榜
    T_increaseshop = T_changeshop %>% arrange(., desc(QSC)) %>% filter(., QSC > 0)
    seventeenall = T_increaseshop_f(T_increaseshop)
    if(nrow(T_increaseshop) > 20) T_increaseshop = T_increaseshop[1:20, ]
    T_increaseshop = zoomin_spword_fun(temp_comment30, T_increaseshop, 'shop', neg = F, asodtm)
    
    # 退步榜
    T_decreaseshop = T_changeshop %>% arrange(., QSC) %>% filter(., QSC < 0)
    eighteenall = T_decreaseshop_f(T_decreaseshop)
    T_decreaseshop$QSC %<>% abs(.)
    if(nrow(T_decreaseshop) > 20) T_decreaseshop = T_decreaseshop[1:20, ]
    T_decreaseshop = zoomin_spword_fun(temp_comment30, T_decreaseshop, 'shop', neg = T, asodtm)
    
    # 30~60
    T_changeshop60 = heroboardForT_fun(temp_comment60, temp_comment90)
    
    # 进步榜
    T_increaseshop60 = T_changeshop60 %>% arrange(., desc(QSC)) %>% filter(., QSC > 0)
    seventeenall60 = T_increaseshop_f(T_increaseshop60)
    if(nrow(T_increaseshop60) > 20) T_increaseshop60 = T_increaseshop60[1:20, ]
    T_increaseshop60 = zoomin_spword_fun(temp_comment60, T_increaseshop60, 'shop', neg = F, asodtm60)
    
    # 退步榜
    T_decreaseshop60 = T_changeshop60 %>% arrange(., QSC) %>% filter(., QSC < 0)
    eighteenall60 = T_decreaseshop_f(T_decreaseshop60)
    T_decreaseshop60$QSC %<>% abs(.)
    if(nrow(T_decreaseshop60) > 20) T_decreaseshop60 = T_decreaseshop60[1:20, ]
    T_decreaseshop60 = zoomin_spword_fun(temp_comment60, T_decreaseshop60, 'shop', neg = T, asodtm60)
    
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  # ----
  
  # 产品----
  tryCatch({
    ln<-'productinf'
    # 近30
    productinf = product_inf_fun(temp_comment30, product)
    
    # 30~60
    productinf60 = product_inf_fun(temp_comment60, product)
    
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 产品次数
  tryCatch({
    ln<-'T_freqproduct'
    # 近30
    temp = arrange(productinf, desc(count))
    if(nrow(temp) > 20) temp = temp[1:20, ]
    T_freqproduct = zoomin_spword_fun(temp_comment30, temp, type = 'product', neg = F, asodtm)
    
    # 30~60
    temp = arrange(productinf60, desc(count))
    if(nrow(temp) > 20) temp = temp[1:20, ]
    T_freqproduct60 = zoomin_spword_fun(temp_comment60, temp, type = 'product', neg = F, asodtm60)
    
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 产品分数
  tryCatch({
    ln<-'T_scoreproduct'
    # 近30
    temp = arrange(productinf, taste)[, c(1, 3)]
    if(nrow(temp) > 20) temp = temp[1:20, ]
    T_scoreproduct = zoomin_spword_fun(temp_comment30, temp, type = 'product', neg = T, asodtm)
    
    # 30~60
    temp = arrange(productinf60, taste)[, c(1, 3)]
    if(nrow(temp) > 20) temp = temp[1:20, ]
    T_scoreproduct60 = zoomin_spword_fun(temp_comment60, temp, type = 'product', neg = T, asodtm60)
    
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 产品情绪
  tryCatch({
    ln <- 'T_sentiproduct'
    # 近30
    temp = arrange(productinf, desc(senti))[, c(1, 6)]
    if(nrow(temp) > 20) temp = temp[1:20, ]
    T_sentiproduct = zoomin_spword_fun(temp_comment30, temp, type = 'product', neg = T, asodtm)
    
    # 30~60
    temp = arrange(productinf60, desc(senti))[, c(1, 6)]
    if(nrow(temp) > 20) temp = temp[1:20, ]
    T_sentiproduct60 = zoomin_spword_fun(temp_comment60, temp, type = 'product', neg = T, asodtm60)
    
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  # ----
  
  # 结论 ----
  # 1 评论趋势图
  tryCatch({
    ln<-'oneall'
    oneall <- T_commenttrend_f(T_allinf, 12)
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 2 三项指标趋势图
  tryCatch({
    ln<-'twoall'
    twoall <- T_qsctrend_f(T_allinf, 12)
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 3 负面评论趋势图
  tryCatch({
    ln<-'threeall'
    threeall <- T_negtrend_f(T_allinf, 12)
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 4 一个帐号的评论次数
  tryCatch({
    ln<-'fourall'
    fourall <- T_usercommentfreq_f(T_userinf)
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 5 词云
  tryCatch({
    ln<-'fiveall'
    fiveall <- T_wordcloud_f(T_wcall)
    fiveall60 <- T_wordcloud_f(T_wcall60)
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 9 提到次数最多的前20项产品
  tryCatch({
    ln<-'nineall'
    nineall <- T_freqproduct_f(productinf)
    nineall60 <- T_freqproduct_f(productinf60)
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 10 味道分数最低的20项产品
  tryCatch({
    ln<-'tenall'
    tenall <- T_scoreproduct_f(productinf)
    tenall60 <- T_scoreproduct_f(productinf60)
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 11 负面评语最多的前20项产品
  tryCatch({
    ln<-'elevenall'
    elevenall <- T_sentiproduct_f(productinf)
    elevenall60 <- T_sentiproduct_f(productinf60)
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 12 提到次数最多20间门店
  tryCatch({
    ln<-'twelveall'
    twelveall <- T_freqshop_f(shopinf)
    twelveall60 <- T_freqshop_f(shopinf60)
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 13 服务分数最低的20间门店
  tryCatch({
    ln<-'thirteenall'
    thirteenall <- T_scoreshop_f(shopinf)
    thirteenall60 <- T_scoreshop_f(shopinf60)
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 14 负面评论最多的20间门店
  tryCatch({
    ln<-'forteenall'
    forteenall <- T_sentishop_f(shopinf)
    forteenall60 <- T_sentishop_f(shopinf60)
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 15 
  tryCatch({
    ln<-'fifteenall'
    fifteenall <- T_labelword_f(T_labelword)
    fifteenall60 <- T_labelword_f(T_labelword60)
    # fifteenall <- data.frame(name = 'T_qscword',
    #                        print = "正面词与负面词")
    # fifteenall60 <- data.frame(name = 'T_qscword',
    #                          print = "正面词与负面词")
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  
  # 16
  tryCatch({
    ln<-'sixteenall'
    sixteenall <- T_allshop_f(T_shopinf)
    sixteenall60 <- T_allshop_f(T_shopinf60)
    logging::loginfo("success",logger = ln)
  }, error = function(e) {
    logging::logerror('error',logger = ln)
  })
  # ----
  
  # 存数据----
  # 存结论
  T_allendprint <- rbind(oneall, twoall, threeall, fourall, fiveall, sevenall, eightall, 
                         nineall, tenall, elevenall, twelveall, thirteenall, forteenall, 
                         fifteenall, sixteenall, seventeenall, eighteenall)
  T_allendprint60 <- rbind(fiveall60, sevenall60, eightall60, 
                           nineall60, tenall60, elevenall60, twelveall60, thirteenall60, forteenall60, 
                             fifteenall60, sixteenall60, seventeenall60, eighteenall60)

  # 当期结论
  m = mongo(collection = 'T_allendprint',db = dbname)
  if (m$count() > 0) m$drop()
  m$insert(T_allendprint)
  # 上期结论
  m = mongo(collection = 'T_allendprint60',db = dbname)
  if (m$count() > 0) m$drop()
  m$insert(T_allendprint60)
  
  # 存数据
  tablename = c('T_allinf', 'T_wcall', 'T_labelword', 'T_increaseshop', 'T_decreaseshop',
                'T_upword', 'T_downword', 'T_freqshop', 'T_scoreshop', 'T_sentishop', 'T_userinf', 
                'T_freqproduct', 'T_scoreproduct', 'T_sentiproduct', 'T_shopinf')
  tablelist = list(T_allinf, T_wcall, T_labelword, T_increaseshop, T_decreaseshop,
                   T_upword, T_downword, T_freqshop, T_scoreshop, T_sentishop, T_userinf, 
                   T_freqproduct, T_scoreproduct, T_sentiproduct, T_shopinf)
  for(i in 1:length(tablename)){
    m = mongo(collection = tablename[i],db = dbname)
    if (m$count() > 0) m$drop()
    m$insert(tablelist[[i]])
  }
  # ----
  Sys.time() - t
}
