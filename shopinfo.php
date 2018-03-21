<?php
ini_set('display_errors',1);
error_reporting(E_ALL);
header("Content-type:text/html;charset=utf-8");

function test($url, $data = NULL, $json = false){
	  $curl = curl_init();
	  curl_setopt($curl, CURLOPT_URL, $url);
      curl_setopt($curl, CURLOPT_SSL_VERIFYPEER, false);
      curl_setopt($curl, CURLOPT_SSL_VERIFYHOST, false);
	   if (!empty($data)) {
		  if($json && is_array($data)){
		      $data = json_encode( $data );
		  }
		  curl_setopt($curl, CURLOPT_POST, 1);
		 curl_setopt($curl, CURLOPT_POSTFIELDS, $data);
		 if($json){ //发送JSON数据
            curl_setopt($curl, CURLOPT_HEADER, 0);
            curl_setopt($curl, CURLOPT_HTTPHEADER,
            array(
                'NAG-Key: 1ffd33375dc54d7c98bbf2033cbc9e6c',
                'NAG-Version: 2.16.28',
                'Content-Type: application/json; charset=utf-8',
                 'Content-Length:' . strlen($data))
            );
		 }
        }
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
        $res = curl_exec($curl);
        $errorno = curl_errno($curl);

        if ($errorno) {
            return array('errorno' => false, 'errmsg' => $errorno);
        }
        curl_close($curl);
        return json_decode($res, true);
}

// 调用  
$url = 'https://api.ipalmap.com/poi/search';
$x = $_GET['x'];
$y = $_GET['y'];
$distance= $_GET['distance'];
$parents = $_GET['parents'];
$data = array('coordinate'=>array('x'=>$x,'y'=>$y),'distance'=>$distance,'parents'=>array('0'=>$parents)); 
//echo json_encode($data);
$response = test($url,$data,true);

echo json_encode($response['list'][0]);

?>
