<?php

/*
 * requirements : int convert to chinese
 */

echo convertIn2Ch(1009) . PHP_EOL;
echo convertIn2Ch(10109) . PHP_EOL;
echo convertIn2Ch(1011) . PHP_EOL;
echo convertIn2Ch(101) . PHP_EOL;
echo convertIn2Ch(910) . PHP_EOL;
echo convertIn2Ch(50). PHP_EOL;
echo convertIn2Ch(500). PHP_EOL;
echo convertIn2Ch(801). PHP_EOL;
function convertIn2Ch($num)
{
    $unitMap = [
      10000 => "万",
      1000 => "千",
      100 => "百",
      10 => "十",
    ];

    $numMap = [
      9 => "九",
      8 => "八",
      7 => "七",
      6 => "六",
      5 => "五",
      4 => "四",
      3 => "三",
      2 => "二",
      1 => "一",
      0 => "零",
    ];

    if ($num < 10) {
        return $numMap[$num];
    }

    $ret = "";
    /*
     * 101 -> 100: 1, 1 -> 10: 0, 1:
     *  1 百: 零 1 -> s > 0 | s = 0
     * 58 -> 5 : 8 -> 0 : 8
     */
    foreach ($unitMap as $k => $v) {
        $s = intval($num / $k);
        $y = intval($num % $k);
        if ($s == 0 && $y >10) {
            continue;
        }

        if ($s > 0){
            $ret .= sprintf("%s%s", $numMap[$s], $v);
          if( strlen($y) != strlen($k)-1 && $y > 0) {
              $ret = $ret . "零";
          } elseif ( $y <= 9 && $y > 0) {
              $ret = $ret . $numMap[$y];
          }
        }elseif ($s == 0 && $y <= 9 && $y > 0) {
            $ret .= sprintf("%s",  $numMap[$y]);
            return $ret;
        }

        $num = $y;
    }



    return $ret;
}