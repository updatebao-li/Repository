<?php
declare(strict_types=1);

/**
 * 综合邮编服务入口
 *
 * - 当输入为 6 位数字时：执行邮编反查逻辑，返回省市区等信息；
 * - 当输入为地址时：调用第三方地图服务解析经纬度并计算邮编。
 *
 * 所有请求都需要提供分配的 key，并统一返回 JSON 结构：
 * {
 *     "msg": string,             // 结果说明
 *     "results": array,          // 查询结果列表
 *     "count": int,              // 已使用次数（包含本次）
 *     "remaincount": int         // 剩余额度，如为负数表示超额
 * }
 */

header('Content-Type: application/json; charset=utf-8');

/* ===================== 授权配置 ===================== */
$API_KEYS = [
    // base64_encode('2023-3-20' . 'youbianku') => 'search_postcode_custom_1,100100'
    'MjAyMy0zLTIwd3d3eW91Ymlw' => ['table' => 'search_postcode_custom_1', 'quota' => 100100],
];

/* ===================== 数据库配置 ===================== */
const SEARCH_LOG_DSN      = 'mysql:host=youbinku.mysql.rds.aliyuncs.com;dbname=search_log;charset=utf8mb4';
const SEARCH_LOG_USER     = '';
const SEARCH_LOG_PASSWORD = '!';

const YOUJIAN_DSN      = 'mysql:host=youbinku.mysql.rds.aliyuncs.com;dbname=youbianku_china;charset=utf8mb4';
const YOUJIAN_USER     = '';
const YOUJIAN_PASSWORD = '!';

const POSTCODE_DSN      = 'mysql:host=127.0.0.1;port=3306;charset=utf8mb4';
const POSTCODE_USER     = 'root';
const POSTCODE_PASSWORD = '123456';
const DB_DIV            = 'china_admin_divisions';
const TB_DIV_SPLIT      = '省市区县切词_copy1';
const DB_STD            = 'china_postcode';
const TB_STD            = 'standard_china_postcode_copy1';

/* ===================== 其他配置 ===================== */
const ALIAS_TSV_PATH = __DIR__ . '/area_alias_map.tsv';
const LIKE_MIN_PREFIX_LEN = 3;
const AREA_SUFFIXES = ['自治州','自治县','市辖区','矿区','林区','新区','地区','特区','经济开发区','开发区','区','县','市','旗','自治旗'];

const GAODE_API_KEY = '1111111111111111';
const BAIDU_API_KEY = '22222222222';
const BAIDU_REVERSE_API_KEY = '2222222222222222';

/* ===================== 入口校验 ===================== */
$input = isset($_GET['address']) ? trim((string)$_GET['address']) : '';
$key   = isset($_GET['key']) ? trim((string)$_GET['key']) : '';
$ip    = $_SERVER['REMOTE_ADDR'] ?? null;

if ($input === '' || $key === '') {
    respondAndExit('身份认证失败', [], 0, 0);
}

if (!isset($API_KEYS[$key])) {
    respondAndExit('身份认证失败', [], 0, 0);
}

$tableName = $API_KEYS[$key]['table'];
$quota     = $API_KEYS[$key]['quota'];

$currentCount = getUsageCount($tableName);
if ($currentCount === null) {
    respondAndExit('服务暂不可用', [], 0, 0);
}

if ($currentCount >= $quota) {
    respondAndExit('余额不足', [], $currentCount, 0);
}

$usageCount  = $currentCount + 1;
$remainCount = $quota - $usageCount;

if (isSixDigitPostcode($input)) {
    handlePostcodeLookup($input, $ip, $tableName, $usageCount, $remainCount);
}

if (!isValidAddress($input)) {
    respondAndExit('地址格式不标准', [], $usageCount, $remainCount);
}

$position = getPosition($input);
if ($position === false) {
    logQuery($ip, $input, '', '', '', '', '', $tableName);
    respondAndExit('地址解析失败', [], $usageCount, $remainCount);
}

$postcode = calculatePostcode($position);
if ($postcode === '') {
    logQuery($ip, $input, '', '', '', '', '', $tableName);
    respondAndExit('地址解析失败', [], $usageCount, $remainCount);
}

logQuery(
    $ip,
    $input,
    $position['formatted_address'] ?? '',
    $position['province'] ?? '',
    $position['district'] ?? '',
    $position['city'] ?? '',
    $postcode,
    $tableName
);

respondAndExit('查询成功', [
    [
        'province' => $position['province'] ?? '',
        'city'     => $position['city'] ?? '',
        'district' => $position['district'] ?? '',
        'postcode' => $postcode,
        'address'  => $position['formatted_address'] ?? '',
    ],
], $usageCount, $remainCount);

/* ===================== 功能函数 ===================== */

/**
 * 统一输出 JSON 并终止脚本。
 */
function respondAndExit(string $msg, array $results, int $count, int $remain): void
{
    echo json_encode([
        'msg'         => $msg,
        'results'     => $results,
        'count'       => $count,
        'remaincount' => $remain,
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    exit;
}

/**
 * 判断是否为 6 位邮编。
 */
function isSixDigitPostcode(string $input): bool
{
    return preg_match('/^\d{6}$/', $input) === 1;
}

/**
 * 地址合法性校验：排除过短、纯字母数字、空输入等情况。
 */
function isValidAddress(string $input): bool
{
    $length = mb_strlen($input, 'UTF-8');
    if ($input === '' || preg_match('/^[a-z0-9#]+$/i', $input)) {
        return false;
    }
    if (mb_strpos($input, '大学', 0, 'UTF-8') !== false) {
        return $length >= 4;
    }
    return $length >= 6;
}

/**
 * 统计 key 的使用量。
 */
function getUsageCount(string $tableName): ?int
{
    try {
        $pdo = new PDO(SEARCH_LOG_DSN, SEARCH_LOG_USER, SEARCH_LOG_PASSWORD, [
            PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        ]);
        $stmt = $pdo->query("SELECT COUNT(guid) AS total FROM `{$tableName}`");
        $row  = $stmt ? $stmt->fetch() : false;
        return $row && isset($row['total']) ? (int)$row['total'] : 0;
    } catch (PDOException $e) {
        return null;
    }
}

/**
 * 记录请求日志。
 */
function logQuery(?string $ip, string $input, string $completeAddress, string $province, string $district, string $city, string $postcode, string $tableName): void
{
    try {
        $pdo = new PDO(SEARCH_LOG_DSN, SEARCH_LOG_USER, SEARCH_LOG_PASSWORD, [
            PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        ]);
        $sql = "INSERT INTO `{$tableName}` (ip, keyword, format_address, search_date, postcode, province, city, area) VALUES (?,?,?,?,?,?,?,?)";
        $stmt = $pdo->prepare($sql);
        $stmt->execute([
            $ip,
            $input,
            $completeAddress,
            date('Y-m-d H:i:s'),
            $postcode,
            $province,
            $city,
            $district,
        ]);
    } catch (PDOException $e) {
        // 日志写入失败不影响主流程
    }
}

/**
 * 调用地图服务解析经纬度及行政区。
 */
function getPosition(string $input)
{
    $input = preg_replace('/\(.*?\)|（.*?）|【.*?】|\[.*?\]/u', '', $input);

    $gaodeUrl = sprintf('https://restapi.amap.com/v3/geocode/geo?key=%s&address=%s', GAODE_API_KEY, urlencode($input));
    $gaode    = httpGetJson($gaodeUrl);

    if (!is_object($gaode) || ($gaode->info ?? '') !== 'OK') {
        return getBaiduPosition($input);
    }

    if (empty($gaode->geocodes[0]->location ?? null)) {
        return false;
    }

    [$lng, $lat] = explode(',', (string)$gaode->geocodes[0]->location);
    $province    = (string)($gaode->geocodes[0]->province ?? '');
    $city        = normalizeCity((string)($gaode->geocodes[0]->city ?? ''));
    $district    = (string)($gaode->geocodes[0]->district ?? '');
    $address     = (string)($gaode->geocodes[0]->formatted_address ?? '');

    return [
        'longitude'         => number_format((float)$lng, 3, '.', ''),
        'latitude'          => number_format((float)$lat, 3, '.', ''),
        'province'          => $province,
        'city'              => $city,
        'district'          => $district,
        'citycode'          => '',
        'formatted_address' => $address,
    ];
}

/**
 * 百度地图兜底解析。
 */
function getBaiduPosition(string $input)
{
    $url   = sprintf('https://api.map.baidu.com/geocoding/v3/?address=%s&output=json&ak=%s', urlencode($input), BAIDU_API_KEY);
    $first = httpGetJsonArray($url);

    if (!isset($first['result']['location']['lng'])) {
        return false;
    }

    $lng = (float)$first['result']['location']['lng'];
    $lat = (float)$first['result']['location']['lat'];

    $reverseUrl = sprintf('http://api.map.baidu.com/reverse_geocoding/v3/?ak=%s&output=json&location=%s,%s', BAIDU_REVERSE_API_KEY, $lat, $lng);
    $reverse    = httpGetJsonArray($reverseUrl);

    if (!isset($reverse['result']['addressComponent'])) {
        return false;
    }

    $component = $reverse['result']['addressComponent'];

    return [
        'longitude'         => number_format($lng, 3, '.', ''),
        'latitude'          => number_format($lat, 3, '.', ''),
        'province'          => (string)($component['province'] ?? ''),
        'city'              => normalizeCity((string)($component['city'] ?? '')),
        'district'          => (string)($component['district'] ?? ''),
        'citycode'          => '',
        'formatted_address' => (string)($reverse['result']['formatted_address'] ?? ''),
    ];
}

/**
 * 统一 GET 请求封装（返回对象）。
 */
function httpGetJson(string $url)
{
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_SSL_VERIFYPEER => false,
        CURLOPT_SSL_VERIFYHOST => false,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_REFERER        => 'https://www.youbianku.com/',
        CURLOPT_HTTPHEADER     => ['Origin: https://www.youbianku.com'],
    ]);
    $resp = curl_exec($ch);
    curl_close($ch);
    return json_decode((string)$resp);
}

/**
 * 统一 GET 请求封装（返回数组）。
 */
function httpGetJsonArray(string $url): array
{
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_SSL_VERIFYPEER => false,
        CURLOPT_SSL_VERIFYHOST => false,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_REFERER        => 'https://www.youbianku.com/',
        CURLOPT_HTTPHEADER     => ['Origin: https://www.youbianku.com'],
    ]);
    $resp = curl_exec($ch);
    curl_close($ch);
    $decoded = json_decode((string)$resp, true);
    return is_array($decoded) ? $decoded : [];
}

/**
 * 市直辖区统一处理。
 */
function normalizeCity(string $city): string
{
    return in_array($city, ['北京市', '天津市', '上海市', '重庆市', ''], true) ? '' : $city;
}

/**
 * 根据经纬度和省市区信息计算邮编。
 */
function calculatePostcode(array $position): string
{
    $district = $position['district'] ?? '';
    if ($district === '北屯市') {
        return '836000';
    }

    try {
        $pdo = new PDO(YOUJIAN_DSN, YOUJIAN_USER, YOUJIAN_PASSWORD, [
            PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        ]);
    } catch (PDOException $e) {
        return '';
    }

    $longitude = (float)($position['longitude'] ?? 0);
    $latitude  = (float)($position['latitude'] ?? 0);
    $province  = $position['province'] ?? '';
    $city      = $position['city'] ?? '';

    $pi    = 3.1415;
    $round = 0.4;
    $limit = 5;
    $dis   = 0.6;

    $piCount        = $pi / 180;
    $latCount       = $latitude * $piCount;
    $lngCount       = $longitude * $piCount;
    $latitudeMin    = $latitude - $round;
    $latitudeMax    = $latitude + $round;
    $longitudeMin   = $longitude - $round;
    $longitudeMax   = $longitude + $round;

    $sql = "SELECT postcode,(SELECT ACOS(SIN({$latCount}) * SIN(`latitude` * {$piCount}) + COS({$latCount}) * COS(`latitude` * {$piCount}) * COS({$lngCount} - `longitude` * {$piCount})) * 6380) AS dis FROM `standard_china_postcode` WHERE `latitude`>{$latitudeMin} AND `latitude`<{$latitudeMax} AND `longitude`>{$longitudeMin} AND `longitude`<{$longitudeMax} AND province=:province AND district=:district ORDER BY dis ASC LIMIT {$limit}";

    $stmt = $pdo->prepare($sql);
    $stmt->execute([':province' => $province, ':district' => $district]);
    $result = $stmt->fetchAll();

    if (!$result) {
        $sqlCity = "SELECT postcode,(SELECT ACOS(SIN({$latCount}) * SIN(`latitude` * {$piCount}) + COS({$latCount}) * COS(`latitude` * {$piCount}) * COS({$lngCount} - `longitude` * {$piCount})) * 6380) AS dis FROM `standard_china_postcode` WHERE `latitude`>{$latitudeMin} AND `latitude`<{$latitudeMax} AND `longitude`>{$longitudeMin} AND `longitude`<{$longitudeMax} AND province=:province AND city=:city ORDER BY dis ASC LIMIT {$limit}";
        $stmt = $pdo->prepare($sqlCity);
        $stmt->execute([':province' => $province, ':city' => $city]);
        $result = $stmt->fetchAll();
    }

    if (!$result) {
        return '';
    }

    $temp = [];
    $postcode = '';

    foreach ($result as $row) {
        $distance = isset($row['dis']) ? (float)$row['dis'] : 0.0;
        if ($distance <= $dis) {
            $postcode = (string)$row['postcode'];
            break;
        }
        $code = (string)$row['postcode'];
        $temp[$code] = ($temp[$code] ?? 0) + 1;
    }

    if ($postcode !== '') {
        return $postcode;
    }

    $maxCount = 0;
    $candidates = [];
    foreach ($temp as $code => $count) {
        if ($count > $maxCount) {
            $maxCount = $count;
            $candidates = [$code];
        } elseif ($count === $maxCount) {
            $candidates[] = $code;
        }
    }

    if (count($candidates) === 1) {
        return (string)$candidates[0];
    }

    foreach ($result as $row) {
        if (in_array($row['postcode'], $candidates, true)) {
            return (string)$row['postcode'];
        }
    }

    return '';
}

/**
 * 处理邮编反查入口。
 */
function handlePostcodeLookup(string $postcode, ?string $ip, string $tableName, int $count, int $remain): void
{
    $details = lookupPostcode($postcode);

    if ($details === null) {
        logQuery($ip, $postcode, '', '', '', '', '', $tableName);
        respondAndExit('未找到匹配的邮编信息', [], $count, $remain);
    }

    logQuery(
        $ip,
        $postcode,
        $details['address'] ?? '',
        $details['province'] ?? '',
        $details['final_area'] ?? '',
        $details['city'] ?? '',
        $postcode,
        $tableName
    );

    respondAndExit('查询成功', [[
        'postcode'    => $postcode,
        'province'    => $details['province'] ?? '',
        'city'        => $details['city'] ?? '',
        'district'    => $details['final_area'] ?? ($details['area'] ?? ''),
        'area_level'  => $details['level'] ?? '',
        'source_note' => $details['source_note'] ?? '',
        'address'     => $details['address'] ?? '',
    ]], $count, $remain);
}

/**
 * 执行邮编数据库查询逻辑。
 */
function lookupPostcode(string $postcode): ?array
{
    try {
        $pdo = new PDO(POSTCODE_DSN, POSTCODE_USER, POSTCODE_PASSWORD, [
            PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        ]);
    } catch (PDOException $e) {
        return null;
    }

    try {
        $stmts = [
            'split_city_exact' => $pdo->prepare("SELECT province, city FROM `" . DB_DIV . "`.`" . TB_DIV_SPLIT . "` WHERE citypostcode=:pc LIMIT 1"),
            'split_area_exact' => $pdo->prepare("SELECT province, city, area FROM `" . DB_DIV . "`.`" . TB_DIV_SPLIT . "` WHERE areapostcode=:pc LIMIT 1"),
            'split_area_like'  => $pdo->prepare("SELECT province, city, area FROM `" . DB_DIV . "`.`" . TB_DIV_SPLIT . "` WHERE areapostcode LIKE :pfx ORDER BY areapostcode LIMIT 1"),
            'split_city_like'  => $pdo->prepare("SELECT province, city FROM `" . DB_DIV . "`.`" . TB_DIV_SPLIT . "` WHERE citypostcode LIKE :pfx ORDER BY citypostcode LIMIT 1"),
            'std_exact'        => $pdo->prepare("SELECT province, city, district, address FROM `" . DB_STD . "`.`" . TB_STD . "` WHERE postcode=:pc LIMIT 1"),
            'std_like'         => $pdo->prepare("SELECT province, city, district, address FROM `" . DB_STD . "`.`" . TB_STD . "` WHERE postcode LIKE :pfx ORDER BY postcode LIMIT 1"),
        ];
    } catch (PDOException $e) {
        return null;
    }

    $result = resolvePostcode($postcode, $stmts, LIKE_MIN_PREFIX_LEN);
    if ($result === null) {
        return null;
    }

    $aliasMap    = loadAliasMap(ALIAS_TSV_PATH);
    $aliasResult = applyAlias($result['province'], $result['city'], $result['area'], $aliasMap, AREA_SUFFIXES);

    $finalArea = $aliasResult['new_area'] ?? $result['area'];

    return [
        'postcode'    => $postcode,
        'province'    => $result['province'],
        'city'        => $result['city'],
        'area'        => $result['area'],
        'final_area'  => $finalArea,
        'level'       => $result['level'],
        'source_note' => $result['source_note'],
        'new_area'    => $aliasResult['new_area'] ?? null,
        'alias_hit'   => $aliasResult['alias_hit'] ?? null,
        'address'     => $result['address'],
    ];
}

/**
 * 以下工具函数移植自原邮编反查脚本。
 */
function resolvePostcode(string $postcode, array $stmts, int $likeMinPrefixLen): ?array
{
    $prov = '';
    $city = '';
    $area = '';
    $level = '';
    $note = '';
    $address = '';

    $last2 = substr($postcode, -2);

    if ($last2 === '00') {
        $stmts['split_area_exact']->execute([':pc' => $postcode]);
        if ($r = $stmts['split_area_exact']->fetch()) {
            $prov = $r['province'] ?? '';
            $city = $r['city'] ?? '';
            $area = $r['area'] ?? '';
            $level = 'area';
            $note = 'area_exact';
        } else {
            $stmts['split_city_exact']->execute([':pc' => $postcode]);
            if ($r = $stmts['split_city_exact']->fetch()) {
                $prov = $r['province'] ?? '';
                $city = $r['city'] ?? '';
                $level = 'city';
                $note = 'city_exact';
            } else {
                for ($len = 5; $len >= $likeMinPrefixLen; $len--) {
                    $prefix = substr($postcode, 0, $len) . '%';
                    $stmts['split_area_like']->execute([':pfx' => $prefix]);
                    if ($x = $stmts['split_area_like']->fetch()) {
                        $prov = $x['province'] ?? '';
                        $city = $x['city'] ?? '';
                        $area = $x['area'] ?? '';
                        $level = 'area';
                        $note = 'like_area_p' . $len;
                        break;
                    }
                    $stmts['split_city_like']->execute([':pfx' => $prefix]);
                    if ($x = $stmts['split_city_like']->fetch()) {
                        $prov = $x['province'] ?? '';
                        $city = $x['city'] ?? '';
                        $level = 'city';
                        $note = 'like_city_p' . $len;
                        break;
                    }
                }
            }
        }
    } else {
        $stmts['std_exact']->execute([':pc' => $postcode]);
        if ($r = $stmts['std_exact']->fetch()) {
            $prov = $r['province'] ?? '';
            $city = $r['city'] ?? '';
            $area = $r['district'] ?? '';
            $level = 'std';
            $note = 'std_exact';
            $address = $r['address'] ?? '';
        } else {
            for ($len = 5; $len >= $likeMinPrefixLen; $len--) {
                $prefix = substr($postcode, 0, $len) . '%';
                $stmts['std_like']->execute([':pfx' => $prefix]);
                if ($x = $stmts['std_like']->fetch()) {
                    $prov = $x['province'] ?? '';
                    $city = $x['city'] ?? '';
                    $area = $x['district'] ?? '';
                    $level = 'std';
                    $note = 'like_std_p' . $len;
                    $address = $x['address'] ?? '';
                    break;
                }
            }
        }
    }

    if ($prov === '' && $city === '' && $area === '') {
        return null;
    }

    return [
        'province'    => $prov,
        'city'        => $city,
        'area'        => $area,
        'level'       => $level,
        'source_note' => $note,
        'address'     => $address,
    ];
}

function loadAliasMap(string $path): array
{
    if (!is_readable($path)) {
        return ['pca' => [], 'pa' => []];
    }
    $fh = fopen($path, 'rb');
    if (!$fh) {
        return ['pca' => [], 'pa' => []];
    }

    $header = tsvReadRow($fh, 7);
    if ($header === false) {
        fclose($fh);
        return ['pca' => [], 'pa' => []];
    }

    $index = [];
    foreach ($header as $i => $name) {
        $index[mb_strtolower(trim((string)$name))] = $i;
    }

    $pca = [];
    $pa  = [];

    while (($row = tsvReadRow($fh, 7)) !== false) {
        $prov = norm((string)($row[$index['province'] ?? -1] ?? ''));
        $city = norm((string)($row[$index['city'] ?? -1] ?? ''));
        $alias = norm((string)($row[$index['alias'] ?? -1] ?? ''));
        $target = norm((string)($row[$index['target_area'] ?? -1] ?? ''));
        if ($prov === '' || $alias === '' || $target === '') {
            continue;
        }
        if ($city !== '') {
            $pca[$prov . '|' . $city . '|' . $alias] = $target;
        }
        $pa[$prov . '|' . $alias] = $target;
    }

    fclose($fh);
    return ['pca' => $pca, 'pa' => $pa];
}

function applyAlias(?string $province, ?string $city, ?string $area, array $aliasMap, array $suffixes): array
{
    $provN = norm((string)$province);
    $cityN = norm((string)$city);
    $areaN = norm((string)$area);

    if ($areaN === '') {
        return ['new_area' => null, 'alias_hit' => null];
    }

    $aliasPCA = $aliasMap['pca'] ?? [];
    $aliasPA  = $aliasMap['pa'] ?? [];

    $newArea = null;
    $aliasHit = null;

    $keyPA = $provN . '|' . $areaN;
    if (isset($aliasPA[$keyPA])) {
        $newArea = $aliasPA[$keyPA];
        $aliasHit = 'pa';
    }

    if ($newArea === null) {
        $core = areaCore($areaN, $suffixes);
        if ($core !== '') {
            foreach ($aliasPA as $key => $val) {
                if (strpos($key, $provN . '|') !== 0) {
                    continue;
                }
                $aliasRaw = substr($key, strlen($provN . '|'));
                $aliasCore = areaCore($aliasRaw, $suffixes);
                if ($aliasCore === $core || ($core !== '' && (mb_strpos($aliasCore, $core) !== false || mb_strpos($core, $aliasCore) !== false))) {
                    $newArea = $val;
                    $aliasHit = ($aliasCore === $core) ? 'pa_core' : 'pa_core_contains';
                    break;
                }
            }
        }
    }

    if ($newArea === null && $cityN !== '') {
        $keyPCA = $provN . '|' . $cityN . '|' . $areaN;
        if (isset($aliasPCA[$keyPCA])) {
            $newArea = $aliasPCA[$keyPCA];
            $aliasHit = 'pca';
        }
    }

    if ($newArea === null && $cityN !== '') {
        $core = $core ?? areaCore($areaN, $suffixes);
        if ($core !== '') {
            foreach ($aliasPCA as $key => $val) {
                if (strpos($key, $provN . '|' . $cityN . '|') !== 0) {
                    continue;
                }
                $aliasRaw = substr($key, strlen($provN . '|' . $cityN . '|'));
                if (areaCore($aliasRaw, $suffixes) === $core) {
                    $newArea = $val;
                    $aliasHit = 'pca_core';
                    break;
                }
            }
        }
    }

    return ['new_area' => $newArea, 'alias_hit' => $aliasHit];
}

function norm(string $s): string
{
    $s = trim($s);
    $s = preg_replace('/\x{3000}|\x{00A0}|\x{FEFF}/u', '', $s);
    $s = preg_replace('/（.*?）|\(.*?\)/u', '', $s);
    return $s;
}

function areaCore(string $name, array $suffixes): string
{
    $s = trim($name);
    foreach ($suffixes as $suffix) {
        $len = mb_strlen($suffix, 'UTF-8');
        if ($len > 0 && mb_substr($s, -$len, null, 'UTF-8') === $suffix) {
            return mb_substr($s, 0, mb_strlen($s, 'UTF-8') - $len, 'UTF-8');
        }
    }
    return $s;
}

function tsvReadRow($fh, int $expect)
{
    $line = fgets($fh);
    if ($line === false) {
        return false;
    }
    $line = rtrim($line, "\r\n");
    if (substr($line, 0, 3) === "\xEF\xBB\xBF") {
        $line = substr($line, 3);
    }
    $cols = explode("\t", $line);
    if (count($cols) < $expect) {
        $cols = array_pad($cols, $expect, '');
    }
    return $cols;
}

