<?php
/**
 * 郵编 → 省市区 → 新旧别名 → 行政区划代码  (纯 TSV I/O 版，杜绝串列)
 *
 * 运行：
 *   php zip_pipeline.php input.txt area_alias_map.tsv
 *
 * 产物（均为 TSV，制表符分隔，无引号）：
 *   - step1.tsv               postcode  province  city  area  level_guess  source_note
 *   - step2_with_alias.tsv    + new_area  alias_hit
 *   - step3_with_codes.tsv    + province_code  city_code  area_code  code_match_note
 */

declare(strict_types=1);

/* ===================== 0) 配置 ===================== */
$DB_HOST    = '127.0.0.1';
$DB_PORT    = 3306;
$DB_USER    = 'root';
$DB_PASS    = '123456';
$DB_CHARSET = 'utf8mb4';

$DB_DIV       = 'china_admin_divisions';
$TB_DIV_SPLIT = '省市区县切词_copy1';                  // province, city, area, areapostcode, citypostcode
$TB_DIV_CODES = 'china_admin_divisions_2023_zhengshi'; // province, province_code, city, city_code, area, area_code

$DB_STD       = 'china_postcode';
$TB_STD       = 'standard_china_postcode_copy1';       // postcode, province, city, district

$INPUT_TXT = $argv[1] ?? 'input.txt';          // 每行一个 6 位邮编
$ALIAS_TSV = $argv[2] ?? 'area_alias_map.tsv'; // TSV：year,province,approved_date,city(可空),alias,target_area,type

$OUT_STEP1 = 'step1.tsv';
$OUT_STEP2 = 'step2_with_alias.tsv';
$OUT_STEP3 = 'step3_with_codes.tsv';

// LIKE 逐位去尾：最多去 3 位（前缀 5 → 4 → 3）
$LIKE_MIN_PREFIX_LEN = 3;

// 区县名常见后缀（做核心名）
$AREA_SUFFIXES = ['自治州','自治县','市辖区','矿区','林区','新区','地区','特区','经济开发区','开发区','区','县','市','旗','自治旗'];

/* ===================== 1) 小工具 ===================== */
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

/** —— 纯 TSV 读写 —— */
/** 写一行 TSV（禁用引号），自动清理内部换行/制表符 */
function tsv_write_row($fh, array $cols): void {
    $safe = [];
    foreach ($cols as $c) {
        $s = (string)$c;
        // 把内嵌 \t 和 \r\n 变成空格，避免破坏列
        $s = str_replace(["\t", "\r\n", "\n", "\r"], ' ', $s);
        $safe[] = $s;
    }
    fwrite($fh, implode("\t", $safe) . "\n");
}
/** 读一行 TSV，返回列数组（按 \t 硬切），并按 $expect 列数进行 pad */
function tsv_read_row($fh, int $expect) {
    $line = fgets($fh);
    if ($line === false) return false;
    $line = rtrim($line, "\r\n");
    // 兼容 Windows/UTF 编码里的 BOM
    if (substr($line, 0, 3) === "\xEF\xBB\xBF") $line = substr($line, 3);
    $cols = explode("\t", $line);
    // pad
    if (count($cols) < $expect) {
        $cols = array_pad($cols, $expect, '');
    }
    return $cols;
}

/** 读取别名 TSV：生成 [pcaMap, paMap] */
function read_tsv_alias(string $path): array {
    if (!file_exists($path)) { fwrite(STDERR, "找不到别名 TSV：{$path}\n"); exit(1); }
    $fh = fopen($path, 'rb'); if (!$fh) { fwrite(STDERR, "无法打开别名 TSV：{$path}\n"); exit(1); }

    $header = tsv_read_row($fh, 7);
    if ($header === false) { fclose($fh); return [[],[]]; }
    // 转小写做列索引
    $idx = [];
    foreach ($header as $i => $name) $idx[mb_strtolower(trim((string)$name))] = $i;

    $pca = []; // 省|市|alias → target_area
    $pa  = []; // 省|alias     → target_area

    while (($row = tsv_read_row($fh, 7)) !== false) {
        $prov = norm((string)($row[$idx['province'] ?? -1] ?? ''));
        $city = norm((string)($row[$idx['city'] ?? -1] ?? ''));
        $ali  = norm((string)($row[$idx['alias'] ?? -1] ?? ''));
        $tgt  = norm((string)($row[$idx['target_area'] ?? -1] ?? ''));
        if ($prov==='' || $ali==='' || $tgt==='') continue;
        if ($city!=='') $pca[$prov.'|'.$city.'|'.$ali] = $tgt;
        $pa[$prov.'|'.$ali] = $tgt;
    }
    fclose($fh);
    echo "别名映射加载：pca=" . count($pca) . "，pa=" . count($pa) . PHP_EOL;
    return [$pca, $pa];
}

/* ===================== 2) DB 连接 & 预编译 ===================== */
$dsn = "mysql:host={$DB_HOST};port={$DB_PORT};charset={$DB_CHARSET}";
$pdo = new PDO($dsn, $DB_USER, $DB_PASS, [
    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
]);

// Step1：split
$stSplitCityExact = $pdo->prepare("SELECT province, city FROM `{$DB_DIV}`.`{$TB_DIV_SPLIT}` WHERE citypostcode=:pc LIMIT 1");
$stSplitAreaExact = $pdo->prepare("SELECT province, city, area FROM `{$DB_DIV}`.`{$TB_DIV_SPLIT}` WHERE areapostcode=:pc LIMIT 1");
$stSplitAreaLike  = $pdo->prepare("SELECT province, city, area FROM `{$DB_DIV}`.`{$TB_DIV_SPLIT}` WHERE areapostcode LIKE :pfx ORDER BY areapostcode LIMIT 1");
$stSplitCityLike  = $pdo->prepare("SELECT province, city FROM `{$DB_DIV}`.`{$TB_DIV_SPLIT}` WHERE citypostcode LIKE :pfx ORDER BY citypostcode LIMIT 1");

// Step1：standard
$stStdExact = $pdo->prepare("SELECT province, city, district FROM `{$DB_STD}`.`{$TB_STD}` WHERE postcode=:pc LIMIT 1");
$stStdLike  = $pdo->prepare("SELECT province, city, district FROM `{$DB_STD}`.`{$TB_STD}` WHERE postcode LIKE :pfx ORDER BY postcode LIMIT 1");

// Step3：codes
$stExactPCA = $pdo->prepare("SELECT province_code, city_code, area_code FROM `{$DB_DIV}`.`{$TB_DIV_CODES}` WHERE province=:p AND city=:c AND area=:a LIMIT 1");
$stExactPA  = $pdo->prepare("SELECT province_code, city_code, area_code FROM `{$DB_DIV}`.`{$TB_DIV_CODES}` WHERE province=:p AND area=:a LIMIT 1");
$stExactPC  = $pdo->prepare("SELECT province_code, city_code FROM `{$DB_DIV}`.`{$TB_DIV_CODES}` WHERE province=:p AND city=:c LIMIT 1");
$stFuzzyPCA = $pdo->prepare("SELECT province_code, city_code, area_code FROM `{$DB_DIV}`.`{$TB_DIV_CODES}` WHERE province=:p AND city=:c AND area LIKE :a LIMIT 1");
$stFuzzyPA  = $pdo->prepare("SELECT province_code, city_code, area_code FROM `{$DB_DIV}`.`{$TB_DIV_CODES}` WHERE province=:p AND area LIKE :a LIMIT 1");

/* ===================== 3) Step1：input.txt → step1.tsv ===================== */
if (!file_exists($INPUT_TXT)) { fwrite(STDERR, "找不到输入文件：{$INPUT_TXT}\n"); exit(1); }
$fin = fopen($INPUT_TXT, 'rb');
$f1  = fopen($OUT_STEP1, 'wb');
if (!$fin || !$f1) { fwrite(STDERR, "无法打开输入或输出文件。\n"); exit(1); }

// 表头
tsv_write_row($f1, ['postcode','province','city','area','level_guess','source_note']);

while (($line = fgets($fin)) !== false) {
    $digits   = preg_replace('/\D+/', '', $line ?? '');
    $postcode = substr($digits ?? '', 0, 6);
    if ($postcode === '' || !preg_match('/^\d{6}$/', $postcode)) {
        tsv_write_row($f1, ['', '', '', '', '', '']);
        continue;
    }

    $prov=''; $city=''; $area=''; $level=''; $note='';
    $last3 = substr($postcode, -3);
    $last2 = substr($postcode, -2);

if ($last2 === '00') {
        // area 精确 → 区级
        $stSplitAreaExact->execute([':pc'=>$postcode]);
        if ($r = $stSplitAreaExact->fetch()) {
            $prov=$r['province']; $city=$r['city']; $area=$r['area']; $level='area'; $note='area_exact';
        } else {
            // city 精确 → 市级
            $stSplitCityExact->execute([':pc'=>$postcode]);
            if ($r = $stSplitCityExact->fetch()) {
                $prov=$r['province']; $city=$r['city']; $level='city'; $note='city_exact';
            } else {
                // LIKE 兜底
                for ($len=5; $len >= $LIKE_MIN_PREFIX_LEN; $len--) {
                    $prefix = substr($postcode, 0, $len) . '%';
                    $stSplitAreaLike->execute([':pfx'=>$prefix]);
                    if ($x = $stSplitAreaLike->fetch()) { $prov=$x['province']; $city=$x['city']; $area=$x['area']; $level='area'; $note="like_area_p{$len}"; break; }
                    $stSplitCityLike->execute([':pfx'=>$prefix]);
                    if ($x = $stSplitCityLike->fetch()) { $prov=$x['province']; $city=$x['city']; $level='city'; $note="like_city_p{$len}"; break; }
                }
            }
        }
    } else {
        // 其它：standard 精确 → LIKE
        $stStdExact->execute([':pc'=>$postcode]);
        if ($r = $stStdExact->fetch()) {
            $prov=$r['province']; $city=$r['city']; $area=$r['district']; $level='std'; $note='std_exact';
        } else {
            for ($len=5; $len >= $LIKE_MIN_PREFIX_LEN; $len--) {
                $prefix = substr($postcode, 0, $len) . '%';
                $stStdLike->execute([':pfx'=>$prefix]);
                if ($x = $stStdLike->fetch()) { $prov=$x['province']; $city=$x['city']; $area=$x['district']; $level='std'; $note="like_std_p{$len}"; break; }
            }
        }
    }

    tsv_write_row($f1, [$postcode,$prov,$city,$area,$level,$note]);
}
fclose($fin);
fclose($f1);
echo "Step1 完成：{$OUT_STEP1}\n";

/* ===================== 4) Step2：别名映射 ===================== */
list($ALIAS_PCA, $ALIAS_PA) = read_tsv_alias($ALIAS_TSV);

$f1r = fopen($OUT_STEP1, 'rb');
$f2  = fopen($OUT_STEP2, 'wb');
if (!$f1r || !$f2) { fwrite(STDERR, "无法打开 Step1 或 Step2 文件。\n"); exit(1); }

// 表头
$hdr1 = tsv_read_row($f1r, 6);
if ($hdr1 === false) { fwrite(STDERR, "Step1 表头无效\n"); exit(1); }
tsv_write_row($f2, array_merge($hdr1, ['new_area','alias_hit']));

while (($row = tsv_read_row($f1r, 6)) !== false) {
    $postcode = $row[0] ?? '';
    $prov     = $row[1] ?? '';
    $city     = $row[2] ?? '';
    $area     = $row[3] ?? '';
    $level    = $row[4] ?? '';
    $src      = $row[5] ?? '';

    $provN = norm($prov);
    $cityN = norm($city);
    $areaN = norm($area);

    $newArea = '';
    $aliasHit= '';

    if ($areaN !== '') {
        // 1) 省 + 区（精确）
        $k2 = $provN.'|'.$areaN;
        if (isset($ALIAS_PA[$k2])) { $newArea = $ALIAS_PA[$k2]; $aliasHit='pa'; }

        // 1b) 省 + 区（核心名/包含）
        if ($newArea === '') {
            $core = areaCore($areaN, $AREA_SUFFIXES);
            foreach ($ALIAS_PA as $key=>$val) {
                if (!starts_with($key, $provN.'|')) continue;
                $aliasRaw  = substr($key, strlen($provN.'|'));
                $aliasCore = areaCore($aliasRaw, $AREA_SUFFIXES);
                if ($aliasCore === $core || ($core!=='' && (mb_strpos($aliasCore,$core)!==false || mb_strpos($core,$aliasCore)!==false))) {
                    $newArea = $val; $aliasHit = ($aliasCore===$core?'pa_core':'pa_core_contains'); break;
                }
            }
        }

        // 2) 省 + 市 + 区（精确）
        if ($newArea === '' && $cityN!=='') {
            $k1 = $provN.'|'.$cityN.'|'.$areaN;
            if (isset($ALIAS_PCA[$k1])) { $newArea = $ALIAS_PCA[$k1]; $aliasHit='pca'; }
        }

        // 2b) 省 + 市 + 区（核心名）
        if ($newArea === '' && $cityN!=='') {
            $core = $core ?? areaCore($areaN, $AREA_SUFFIXES);
            foreach ($ALIAS_PCA as $key=>$val) {
                if (!starts_with($key, $provN.'|'.$cityN.'|')) continue;
                $aliasRaw = substr($key, strlen($provN.'|'.$cityN.'|'));
                if (areaCore($aliasRaw, $AREA_SUFFIXES) === $core) { $newArea = $val; $aliasHit='pca_core'; break; }
            }
        }
    }

    tsv_write_row($f2, [$postcode,$prov,$city,$area,$level,$src,$newArea,$aliasHit]);
}
fclose($f1r);
fclose($f2);
echo "Step2 完成：{$OUT_STEP2}\n";

/* ===================== 5) Step3：行政区划代码 ===================== */
$f2r = fopen($OUT_STEP2, 'rb');
$f3  = fopen($OUT_STEP3, 'wb');
if (!$f2r || !$f3) { fwrite(STDERR, "无法打开 Step2 或 Step3 文件。\n"); exit(1); }

// 表头
$hdr2 = tsv_read_row($f2r, 8);
if ($hdr2 === false) { fwrite(STDERR, "Step2 表头无效\n"); exit(1); }
tsv_write_row($f3, array_merge($hdr2, ['province_code','city_code','area_code','code_match_note']));

while (($row = tsv_read_row($f2r, 8)) !== false) {
    $postcode = $row[0] ?? '';
    $prov     = $row[1] ?? '';
    $city     = $row[2] ?? '';
    $area     = $row[3] ?? '';
    $newArea  = $row[6] ?? '';
    $finalArea= $newArea !== '' ? $newArea : $area;

    $provCode=''; $cityCode=''; $areaCode=''; $note='';

    // A) 省+市+区（精确）
    if ($prov!=='' && $city!=='' && $finalArea!=='') {
        $stExactPCA->execute([':p'=>$prov, ':c'=>$city, ':a'=>$finalArea]);
        if ($r = $stExactPCA->fetch()) {
            $provCode=$r['province_code']??''; $cityCode=$r['city_code']??''; $areaCode=$r['area_code']??''; $note='';
            tsv_write_row($f3, array_merge($row, [$provCode,$cityCode,$areaCode,$note])); continue;
        }
        // B) 模糊（核心名 LIKE）
        $core = areaCore($finalArea, $AREA_SUFFIXES);
        if ($core!=='') {
            $stFuzzyPCA->execute([':p'=>$prov, ':c'=>$city, ':a'=>'%'.$core.'%']);
            if ($r = $stFuzzyPCA->fetch()) {
                $provCode=$r['province_code']??''; $cityCode=$r['city_code']??''; $areaCode=$r['area_code']??''; $note='fuzzy_pca';
                tsv_write_row($f3, array_merge($row, [$provCode,$cityCode,$areaCode,$note])); continue;
            }
        }
    }

    // C) 省+区（精确 / 模糊）
    if ($prov!=='' && $finalArea!=='') {
        $stExactPA->execute([':p'=>$prov, ':a'=>$finalArea]);
        if ($r = $stExactPA->fetch()) {
            $provCode=$r['province_code']??''; $cityCode=$r['city_code']??''; $areaCode=$r['area_code']??''; $note='';
            tsv_write_row($f3, array_merge($row, [$provCode,$cityCode,$areaCode,$note])); continue;
        }
        $core = $core ?? areaCore($finalArea, $AREA_SUFFIXES);
        if ($core!=='') {
            $stFuzzyPA->execute([':p'=>$prov, ':a'=>'%'.$core.'%']);
            if ($r = $stFuzzyPA->fetch()) {
                $provCode=$r['province_code']??''; $cityCode=$r['city_code']??''; $areaCode=$r['area_code']??''; $note='fuzzy_pa';
                tsv_write_row($f3, array_merge($row, [$provCode,$cityCode,$areaCode,$note])); continue;
            }
        }
    }

    // D) 省+市（精确）
    if ($prov!=='' && $city!=='') {
        $stExactPC->execute([':p'=>$prov, ':c'=>$city]);
        if ($r = $stExactPC->fetch()) {
            $provCode=$r['province_code']??''; $cityCode=$r['city_code']??''; $note='';
            tsv_write_row($f3, array_merge($row, [$provCode,$cityCode,'',$note])); continue;
        }
    }

    // 未命中
    tsv_write_row($f3, array_merge($row, [$provCode,$cityCode,$areaCode,$note]));
}
fclose($f2r);
fclose($f3);
echo "Step3 完成：{$OUT_STEP3}\n";
