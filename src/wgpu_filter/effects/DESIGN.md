# wgpu_filter::effects 设计文档

> **模块画像**:适配器

## 架构

`effects` 把「一份固定的 WGSL shader 契约」适配成 6 个命名特效:所有特效共享同一个
`@group(0)` 头部(输入纹理/采样器/`ez` uniforms,即 `shader_wgsl` 的标准契约)与同一条
底层管线(`WgpuFrameFilter`),差异只在各自的 `@group(1)` 参数结构和 fragment 主体。
对外每个特效暴露一个构造函数(`adjust(params)` 等),返回类型化 builder;build 产物
`Effect<P>` 通过委托实现 `FrameFilter`,并把参数类型 `P` 固化进 handle 获取路径。

### 静态分层

```text
┌────────────────────────────────────────────────────────┐
│  调用方(frame pipeline)                                │
│      ↓ 构造函数:adjust / beauty / sharpen / ...       │
├────────────────────────────────────────────────────────┤
│  类型化 builder 层                                      │
│    EffectBuilder<P>(通用直通旋钮)                      │
│    BeautyBuilder(额外持有 build 期 quality 选择)       │
│      ↓ shader 组装(EFFECT_HEADER + 特效主体)          │
├────────────────────────────────────────────────────────┤
│  Effect<P>(FrameFilter 委托 + 类型化 params_handle)   │
│      ↓ 全部转发                                         │
├────────────────────────────────────────────────────────┤
│  wgpu_frame_filter(底层 GPU 管线,本模块不触碰)       │
└────────────────────────────────────────────────────────┘
```

### 业务实体关系

```text
构造函数(6 个) ──1:1──▶ 参数结构(#[repr(C)] Pod)
      │                        │ 位置对应(字段序即布局)
      ▼                        ▼
EffectBuilder<P> ──build──▶ Effect<P> ──1:1──▶ WgpuFrameFilter(内嵌)
                               │
                               └──N 个──▶ WgpuParamsHandle<P>(任意线程实时调参)
```

- **构造函数** — 特效的唯一入口,把预设参数和对应 WGSL 主体绑进 builder
- **参数结构** — 全标量(f32/u32)、16 字节倍数,与 WGSL uniform 结构逐位置镜像
- **Effect<P>** — 参数类型在 build 时固化,handle 获取不再需要 turbofish,也
  无法与同尺寸的异类结构误配

关系本质:类型参数 `P` 是构造函数到 handle 的映射纽带,把「参数结构 ↔ shader
uniform」的对应从运行期检查(字节数比对)提前到编译期(类型绑定)。

## 契约矩阵

| 特效 | 参数结构(字节) | 邻域采样 | 核大小 | 备注 |
|---|---|---|---|---|
| `adjust` | AdjustParams(32) | 无(单像素) | — | 8 控制项一遍完成 |
| `beauty` / `portrait` | BeautyParams(16) | 有 | Fast=9 / Balanced=13 taps(build 期烤入) | 肤色 mask 门控;`portrait` 是更强预设 |
| `sharpen` | SharpenParams(16) | 有 | 5 taps | 软阈值噪声门 |
| `transform` | TransformParams(32) | 无(逆映射) | — | 出帧渲染黑;正角=观察者逆时针 |
| `pixelate` | PixelateParams(16) | 无(块中心取样) | — | 块裁剪到帧内后取中心;`block_size<=1` 退化为直通 |
| `soft_blur` | SoftBlurParams(16) | 有 | 13 taps 双环盘 | `privacy()` 强预设 |

全矩阵共享的契约:
- 邻域步长一律取自 `textureDimensions(texture1)`(输入尺寸);`ez.width/height`
  是输出尺寸,resize 时二者不同,用错会让核间距随输出缩放漂移。
- 参数在 shader 内 clamp 到文档区间;宿主侧不校验、不报错。
- WGSL 结构与宿主结构按**字段位置**镜像(全标量,两侧都无隐式 padding;
  bytemuck derive 在编译期证明宿主侧无 padding 字节)。字段名允许不同——
  `smooth` 是 WGSL 保留字,shader 侧改名 `smoothing`。

## 关键决策与权衡

1. **类型化 builder,不用总 enum**。备选方案是 `EffectKind` 枚举 + 统一
   `EffectConfig`,但那会造成异构参数挤一个类型、版本演进互相牵连、live handle
   类型擦除三个问题。代价是每个特效多一个构造函数与参数结构——接受。

2. **`BeautyQuality` 在 build 期烤进 shader,不提供运行期 tap 数**。GPU wavefront
   对动态循环上界仍按最大值调度,运行期参数省不下算力;两个 quality 就是两份
   shader 源文本。

3. **参数越界 clamp 而非报错**。实时调参场景下,一次 `update` 传出界值如果让
   管线报错,代价远大于画面短暂钳制;`build()` 因此对参数值零校验(尺寸/对齐
   仍由底层 builder 校验)。

4. **padding 字段 `#[doc(hidden)] pub`**。私有 padding 会破坏
   `..Default::default()` 函数式更新语法(E0451);`#[non_exhaustive]` 同样破坏。
   公开但文档隐藏是唯一保住该语法的选择,代价是调用方理论上可写入 padding
   (shader 不读,无害)。

5. **`Effect::params_handle` 用 `expect` 而非返回 Result**。不变量:`Effect<P>`
   唯一构造路径是 `EffectBuilder::build`,它必然以 `size_of::<P>()` 字节初始化
   底层参数;`SharedParams.len` 构造后不变。尺寸不匹配分支不可达,把不可达错误
   传染给调用方签名不值得。

## 失败契约

- **`build()`** → `WgpuFilterError::InvalidOption`。经由本模块 API 实际可达的只有
  底层 builder 的通用校验(如 `frames_in_flight` 出范围);shader 与参数尺寸由
  本模块构造,恒合法。
- **管线 `init`** → 无 GPU adapter / 设备创建失败,以 `FrameFilterError` 冒泡
  (与手写 shader 的 `WgpuFrameFilter` 完全一致,本模块零新增失败模式)。
- **运行期** → 超限帧尺寸、不支持的像素格式等,同上由底层管线报告。
- **`params_handle` / `update`** → 不失败;锁中毒时接管字节缓冲(始终处于合法
  状态)。重入死锁是文档化禁区,不是运行期检查。

## 并发模型

参数通道 = `Arc<Mutex<Vec<u8>>>` + `AtomicBool` 脏标记(定义在上游 `params`
模块,本模块只消费)。`set` 整体替换;`update` 在锁内做读-改-写,保证并发
`set`/`update` 串行化不丢写。`Effect<P>` 的 `FrameFilter` 实现要求 `P: Send`
(handle 可能被送去其他线程)。本模块自身不 spawn 线程、不持长锁。

## 测试策略

- **离线 shader 校验**:naga(与 wgpu 同大版本)解析+验证全部 7 份组装产物
  (beauty 两档 × 1 + 其余 5),shader 文本错误在 `cargo test` 就炸,不等真 GPU。
- **布局哨兵**:每个参数结构的 `size_of` 钉死为 16 的倍数(WGSL uniform 结构
  尺寸向 16 取整,宿主必须一致)。
- **GPU 运行期 oracle**(无 adapter 时跳过):中性参数 vs identity 管线逐平面
  ≤1~2 码值;镜像换边、正角旋转亮边落顶(方向契约)、马赛克块内平坦+超帧块
  取帧中心、`block_size=1`+resize 与 identity resize 一致、sharpen 边缘过冲、
  美颜常量帧保持均匀、`update` 降饱和实时生效。
