## 贡献

贡献方式与主分支相同。请确保在 shade 分支上进行与依赖管理和 shaded JAR 相关的改进。

## 许可证

本项目采用 Apache License 2.0 许可证 - 详情请见 [LICENSE](LICENSE) 文件。
## 依赖重定位

在 shade 过程中，以下包被重定位以避免冲突：

- `com.aliyun` -> `shaded.com.aliyun`
- `org.apache.http` -> `shaded.org.apache.http`
- 其他潜在冲突的第三方库

这确保了即使 Hadoop 环境中有不同版本的相同库，连接器也能正常工作。

## 注意事项

1. **文件大小**: Shaded JAR 文件比普通 JAR 大得多，因为它包含了所有依赖项。

2. **调试**: 由于类被重定位，调试时可能需要考虑这一点。

3. **兼容性**: 虽然 shade 版本解决了依赖冲突问题，但应确保它与您的 Hadoop 版本兼容。

## 故障排除

### ClassNotFoundException

如果遇到 `ClassNotFoundException`，请确保：
1. Shaded JAR 文件已正确部署到类路径
2. Hadoop 进程已重启以加载新 JAR

### 版本冲突

如果仍然遇到版本冲突：
1. 检查是否还有其他版本的 JAR 在类路径中
2. 确保使用的是 shade 版本而不是标准版本

## 构建配置

Shade 构建使用 Maven Shade Plugin，配置如下：

### 配置

配置与标准版本相同。请参考主 README 中的配置部分。

### 使用示例

这将生成一个包含所有依赖项的 JAR 文件，通常命名为 `hadoop-oss-connector-v2-shaded-{version}.jar`。

### 构建参数

| 参数 | 描述 |
|------|------|
| `-Pshade` | 激活 shade 构建配置文件 |
| `-DskipTests` | 跳过测试以加快构建过程 |

## 使用

### 部署

将生成的 shaded JAR 文件复制到 Hadoop 集群的类路径中：

# Hadoop OSS Connector - Shade Branch

Hadoop OSS Connector V2 Shade 是 Hadoop OSS Connector 的一个特殊构建版本，它将所有依赖项打包到一个单一的"uber" JAR文件中，以避免类路径冲突并简化部署。

## 目的

shade 分支的主要目的是创建一个独立的 JAR 文件，其中包含连接器及其所有依赖项，从而：
- 避免与 Hadoop 集群中已有的库发生版本冲突
- 简化部署过程，只需要一个 JAR 文件
- 确保在各种 Hadoop 发行版中的一致性

## 特性

- 包含所有必需的依赖项（如 Alibaba Cloud SDK）
- 重定位冲突的依赖项以避免类路径问题
- 与标准 Hadoop OSS Connector 功能完全相同
- 更容易集成到不同的 Hadoop 环境中

## 构建

### 构建 Shade JAR

