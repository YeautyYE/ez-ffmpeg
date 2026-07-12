# wgpu_filter::effects

内置命名 GPU 特效目录:每个特效是一个类型化构造器,返回绑定了参数类型的
[`Effect<P>`](mod.rs),可直接装进 frame pipeline,运行中通过 typed handle 实时调参。

## 用途

把「常用直播/短视频特效」沉淀为库 API:调用方不写 WGSL、不关心 uniform 布局,
用 Rust 结构体表达参数,一行构造出可入管线的 `FrameFilter`。

### 非目标

- 不做多特效单实例融合链(每个 `Effect` 独占一条完整 GPU 管线;见「注意事项」)
- 不做人脸检测/分割级美颜(`beauty` 是启发式肤色 mask 的 lite 档,命名即声明)
- 不接受运行期改采样核大小(`BeautyQuality` 在 build 时烤进 shader)

## 快速开始

```rust,ignore
use ez_ffmpeg::wgpu_filter::effects::{adjust, AdjustParams};
use ez_ffmpeg::filter::frame_pipeline_builder::FramePipelineBuilder;
use ez_ffmpeg::AVMediaType;

let effect = adjust(AdjustParams {
    saturation: 1.2,
    temperature: 0.15,
    ..AdjustParams::default()
})
.build()?;

let pipeline: FramePipelineBuilder = AVMediaType::AVMEDIA_TYPE_VIDEO.into();
let pipeline = pipeline.filter("adjust", Box::new(effect));
// output.add_frame_pipeline(pipeline);
```

所有参数结构的 `Default` 都是中性或温和预设:默认构造的特效不会明显改变画面。

## 使用示例

**直播美颜(集显上用 Fast 档)**:

```rust,ignore
use ez_ffmpeg::wgpu_filter::effects::{beauty, BeautyParams, BeautyQuality};

let effect = beauty(BeautyParams::default())
    .quality(BeautyQuality::Fast)   // 9 taps;集显 1080p60
    .frames_in_flight(1)            // 直播低延迟
    .build()?;
```

或直接用融合预设 `portrait()`(磨皮+美白+提亮一档到位)。

**运行中实时调参**(任意线程):

```rust,ignore
let effect = adjust(AdjustParams::default()).build()?;
let params = effect.params_handle(); // 类型已绑定,无需 turbofish
// ...特效已装入管线并运行...
params.update(|p| p.saturation = 0.0); // 原子读改写,下一帧生效
```

**隐私模糊 + 缩小输出**:

```rust,ignore
use ez_ffmpeg::wgpu_filter::effects::{soft_blur, SoftBlurParams};

let effect = soft_blur(SoftBlurParams::privacy())
    .output_size(640, 360)
    .build()?;
```

**镜像(自拍翻转)**:

```rust,ignore
use ez_ffmpeg::wgpu_filter::effects::{transform, TransformParams};

let effect = transform(TransformParams::mirrored()).build()?;
```

目录一览:`adjust`(亮度/对比度/饱和度/曝光/gamma/自然饱和/白平衡 8 控制项)、
`beauty`/`portrait`(磨皮+美白+提亮)、`sharpen`(luma 锐化)、`transform`
(镜像/翻转/旋转/缩放/平移)、`pixelate`(马赛克)、`soft_blur`(柔焦/隐私模糊)。

## 依赖

- **上游**:`wgpu_filter::wgpu_frame_filter`(底层 GPU 管线与 builder)、
  `wgpu_filter::params`(`WgpuParamsHandle` 实时参数)、
  `wgpu_filter::error`(`WgpuFilterError`)、`core::filter::frame_filter`
  (`FrameFilter` trait,委托实现)、`bytemuck`(参数结构 Pod 派生)
- **下游**:无仓内调用方(公开 API,面向 crate 使用者)
- **外部**:随 `wgpu` feature 启用;运行期需要一个 wgpu 可用的 GPU adapter

## 注意事项

- **一个特效 = 一条完整 GPU 管线**(上传/转换/特效/打包/回读)。串联两个
  `Effect` 意味着这套开销 ×2。优先选参数覆盖面广的单特效(`adjust` 八项、
  `portrait` 融合),不要叠实例。
- **参数越界不报错**:所有参数在 shader 内 clamp 到文档区间,`build()` 不校验
  数值——传 `saturation: 99.0` 得到的是 clamp 后的 3.0 效果。
- **`_pad` 字段必须保持 0**:参数结构的 padding 字段是 `#[doc(hidden)] pub`
  (为了 `..Default::default()` 语法),不要写入。
- **`params_handle().update(|p| ...)` 闭包内不要再调同一特效的 handle 方法**
  ——锁跨闭包持有,重入会死锁。
- **`BeautyQuality` 只能 build 时选**:Fast=9 taps(集显 1080p60),
  Balanced=13 taps(独显或 720p)。运行期 tap 数会让 GPU wavefront 仍跑满
  整个循环,故不提供。
- **无 GPU 环境**:`build()` 纯 CPU 侧(shader 组装+校验),不触 GPU;
  设备创建发生在管线 `init`,无 adapter 时在那里报错。
