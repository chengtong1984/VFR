CREATE TABLE `campaign_info` (
  `viooh_id` varchar(50) DEFAULT NULL,
  `campaign_id` int(11) DEFAULT NULL,
  `sb_id` varchar(20) DEFAULT NULL,
  `customer_name` varchar(50) DEFAULT NULL,
  `start` date DEFAULT NULL,
  `end` date DEFAULT NULL,
  `content_length` int(11) DEFAULT NULL
)

CREATE TABLE `daysum_info` (
  `campaign_id` int(11) DEFAULT NULL,
  `count_num` int(11) DEFAULT NULL,
  `screen_num` int(11) DEFAULT NULL,
  `show_time` date DEFAULT NULL
)

CREATE TABLE `player_info` (
  `player_name` varchar(30) DEFAULT NULL,
  `player_id` int(11) DEFAULT NULL
)
