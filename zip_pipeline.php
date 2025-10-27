<?php
/**
 * 邮编解析 API 脚本
 *
 * 提供根据 6 位邮编查询省市区信息的接口，包含别名映射，并在可用时返回地址。
 *
 * 使用方式：
 *   - 作为 HTTP 接口：部署到支持 PHP 的服务器，访问 ?postcode=XXXXXX
 *   - 作为 CLI 工具： php zip_pipeline.php 123456
 */

declare(strict_types=1);

$IS_CLI = PHP_SAPI === 'cli';
if (!$IS_CLI) {
    header('Content-Type: application/json; charset=utf-8');
}

/* ===================== 配置 ===================== */
$DB_HOST    = '127.0.0.1';
$DB_PORT    = 3306;
$DB_USER    = 'root';
$DB_PASS    = '123456';
$DB_CHARSET = 'utf8mb4';

$DB_DIV       = 'china_admin_divisions';
$TB_DIV_SPLIT = '省市区县切词_copy1';                  // province, city, area, areapostcode, citypostcode

$DB_STD       = 'china_postcode';
$TB_STD       = 'standard_china_postcode_copy1';       // postcode, province, city, district, address

$ALIAS_TSV = __DIR__ . '/area_alias_map.tsv';          // 可选，若不存在则不进行别名转换

$LIKE_MIN_PREFIX_LEN = 3;                              // LIKE 逐位去尾：最多去 3 位（前缀 5 → 4 → 3）
$AREA_SUFFIXES = ['自治州','自治县','市辖区','矿区','林区','新区','地区','特区','经济开发区','开发区','区','县','市','旗','自治旗'];

/* ===================== 工具函数 ===================== */
function jsonResponse(bool $success, string $message, ?array $data = null, int $statusCode = 200): void {
    global $IS_CLI;
    $payload = [
        'success' => $success,
        'message' => $message,
        'data'    => $data,
    ];
    if (!$IS_CLI) {
        http_response_code($statusCode);
    }
    echo json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . ($IS_CLI ? PHP_EOL : '');
    exit;
}

function getRequestedPostcode(): string {
    global $IS_CLI;
    if ($IS_CLI) {
        global $argv;
        return trim((string)($argv[1] ?? ''));
    }
    $method = $_SERVER['REQUEST_METHOD'] ?? 'GET';
    if ($method === 'POST') {
        return trim((string)($_POST['postcode'] ?? ''));
    }
    return trim((string)($_GET['postcode'] ?? ''));
}

function starts_with(string $haystack, string $prefix): bool {
    return $prefix === '' || strncmp($haystack, $prefix, strlen($prefix)) === 0;
}

/** 归一化：去空白/全角空格/NBSP/BOM/括注 */
function norm(string $s): string {
    $s = trim($s);
    $s = preg_replace('/\x{3000}/u', '', $s); // 全角空格
    $s = preg_replace('/\x{00A0}/u', '', $s); // NBSP
    $s = preg_replace('/\x{FEFF}/u', '', $s); // BOM/ZWNBSP
    $s = preg_replace('/（.*?）|\(.*?\)/u', '', $s); // 括注
    return $s;
}

/** 取“核心名”：去常见尾缀 */
function areaCore(string $name, array $suffixes): string {
    $s = trim($name);
    foreach ($suffixes as $suf) {
        $len = mb_strlen($suf, 'UTF-8');
        if ($len && mb_substr($s, -$len, null, 'UTF-8') === $suf) {
            return mb_substr($s, 0, mb_strlen($s, 'UTF-8') - $len, 'UTF-8');
        }
    }
    return $s;
}

function emptyToNull(?string $value): ?string {
    $value = trim((string)($value ?? ''));
    return $value === '' ? null : $value;
}

/** 读取别名 TSV：生成 ['pca'=>..., 'pa'=>...] */
function loadAliasMap(string $path): array {
    if (!is_readable($path)) {
        return ['pca' => [], 'pa' => []];
    }
    $fh = fopen($path, 'rb');
    if (!$fh) {
        return ['pca' => [], 'pa' => []];
    }
    $header = tsv_read_row($fh, 7);
    if ($header === false) {
        fclose($fh);
        return ['pca' => [], 'pa' => []];
    }
    $idx = [];
    foreach ($header as $i => $name) {
        $idx[mb_strtolower(trim((string)$name))] = $i;
    }

    $pca = [];
    $pa  = [];

    while (($row = tsv_read_row($fh, 7)) !== false) {
        $prov = norm((string)($row[$idx['province'] ?? -1] ?? ''));
        $city = norm((string)($row[$idx['city'] ?? -1] ?? ''));
        $ali  = norm((string)($row[$idx['alias'] ?? -1] ?? ''));
        $tgt  = norm((string)($row[$idx['target_area'] ?? -1] ?? ''));
        if ($prov === '' || $ali === '' || $tgt === '') continue;
        if ($city !== '') $pca[$prov . '|' . $city . '|' . $ali] = $tgt;
        $pa[$prov . '|' . $ali] = $tgt;
    }
    fclose($fh);

    return ['pca' => $pca, 'pa' => $pa];
}

/** 读一行 TSV，返回列数组（按 \t 硬切），并按 $expect 列数进行 pad */
function tsv_read_row($fh, int $expect) {
    $line = fgets($fh);
    if ($line === false) return false;
    $line = rtrim($line, "\r\n");
    if (substr($line, 0, 3) === "\xEF\xBB\xBF") $line = substr($line, 3);
    $cols = explode("\t", $line);
    if (count($cols) < $expect) {
        $cols = array_pad($cols, $expect, '');
    }
    return $cols;
}

/**
 * 根据邮编解析基础省市区信息
 * @return array{province:string,city:string,area:string,level:string,source_note:string,address:string}|null
 */
function resolvePostcode(string $postcode, array $stmts, int $likeMinPrefixLen): ?array {
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

/**
 * 根据别名映射返回新的区县名称
 * @return array{new_area:?string,alias_hit:?string}
 */
function applyAlias(?string $province, ?string $city, ?string $area, array $aliasMap, array $suffixes): array {
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

    $k2 = $provN . '|' . $areaN;
    if (isset($aliasPA[$k2])) {
        $newArea = $aliasPA[$k2];
        $aliasHit = 'pa';
    }

    if ($newArea === null) {
        $core = areaCore($areaN, $suffixes);
        if ($core !== '') {
            foreach ($aliasPA as $key => $val) {
                if (!starts_with($key, $provN . '|')) continue;
                $aliasRaw  = substr($key, strlen($provN . '|'));
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
        $k1 = $provN . '|' . $cityN . '|' . $areaN;
        if (isset($aliasPCA[$k1])) {
            $newArea = $aliasPCA[$k1];
            $aliasHit = 'pca';
        }
    }

    if ($newArea === null && $cityN !== '') {
        $core = $core ?? areaCore($areaN, $suffixes);
        if ($core !== '') {
            foreach ($aliasPCA as $key => $val) {
                if (!starts_with($key, $provN . '|' . $cityN . '|')) continue;
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

/* ===================== 主逻辑 ===================== */
$postcodeRaw = getRequestedPostcode();
if ($postcodeRaw === '') {
    jsonResponse(false, '缺少邮编参数', null, 400);
}

$digits = preg_replace('/\D+/', '', $postcodeRaw);
if (!preg_match('/^\d{6}$/', $digits)) {
    jsonResponse(false, '邮编格式错误，应为 6 位数字', null, 422);
}

try {
    $dsn = "mysql:host={$DB_HOST};port={$DB_PORT};charset={$DB_CHARSET}";
    $pdo = new PDO($dsn, $DB_USER, $DB_PASS, [
        PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    ]);
} catch (PDOException $e) {
    jsonResponse(false, '数据库连接失败', null, 500);
}

try {
    $stmts = [
        'split_city_exact' => $pdo->prepare("SELECT province, city FROM `{$DB_DIV}`.`{$TB_DIV_SPLIT}` WHERE citypostcode=:pc LIMIT 1"),
        'split_area_exact' => $pdo->prepare("SELECT province, city, area FROM `{$DB_DIV}`.`{$TB_DIV_SPLIT}` WHERE areapostcode=:pc LIMIT 1"),
        'split_area_like'  => $pdo->prepare("SELECT province, city, area FROM `{$DB_DIV}`.`{$TB_DIV_SPLIT}` WHERE areapostcode LIKE :pfx ORDER BY areapostcode LIMIT 1"),
        'split_city_like'  => $pdo->prepare("SELECT province, city FROM `{$DB_DIV}`.`{$TB_DIV_SPLIT}` WHERE citypostcode LIKE :pfx ORDER BY citypostcode LIMIT 1"),
        'std_exact'        => $pdo->prepare("SELECT province, city, district, address FROM `{$DB_STD}`.`{$TB_STD}` WHERE postcode=:pc LIMIT 1"),
        'std_like'         => $pdo->prepare("SELECT province, city, district, address FROM `{$DB_STD}`.`{$TB_STD}` WHERE postcode LIKE :pfx ORDER BY postcode LIMIT 1"),
    ];
} catch (PDOException $e) {
    jsonResponse(false, '初始化查询语句失败', null, 500);
}

$result = resolvePostcode($digits, $stmts, $LIKE_MIN_PREFIX_LEN);
if ($result === null) {
    jsonResponse(false, '未找到匹配的邮编信息', null, 404);
}

$aliasMap = loadAliasMap($ALIAS_TSV);
$aliasResult = applyAlias($result['province'], $result['city'], $result['area'], $aliasMap, $AREA_SUFFIXES);

$finalArea = $aliasResult['new_area'] ?? $result['area'];

$responseData = [
    'postcode'    => $digits,
    'province'    => emptyToNull($result['province']),
    'city'        => emptyToNull($result['city']),
    'area'        => emptyToNull($result['area']),
    'final_area'  => emptyToNull($finalArea),
    'level'       => emptyToNull($result['level']),
    'source_note' => emptyToNull($result['source_note']),
    'new_area'    => emptyToNull($aliasResult['new_area'] ?? null),
    'alias_hit'   => emptyToNull($aliasResult['alias_hit'] ?? null),
    'address'     => emptyToNull($result['address']),
];

jsonResponse(true, 'OK', $responseData, 200);
