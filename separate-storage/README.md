# Separate Storage 이미지 최적화 Lambda

원본 이미지와 최적화된 이미지를 분리해서 저장하는 방식입니다.

## 특징

- 원본 이미지 보존
- 다양한 크기와 형식의 이미지 생성
- WebP 변환 지원
- 반응형 이미지 지원
- 썸네일 자동 생성

## S3 버킷 구조

```
bucket/
├── original/         # 원본 이미지 (presigned URL로 업로드)
├── optimized/        # 최적화된 이미지 (최대 1200px, 비율 유지)
├── thumbnails/       # 썸네일 (최대 300px, 비율 유지)
├── square-thumbnails/ # 정사각형 썸네일 (300x300, 중앙 크롭)
├── sizes/           # 다양한 크기 (320px, 640px, 1024px)
└── webp/            # WebP 형식 변환본
```

## 설정

### Lambda 함수 설정

- Runtime: Node.js 18.x
- Memory: 1536 MB (여러 이미지 생성을 위해 더 많은 메모리)
- Timeout: 10분
- Environment Variables:
  - `NODE_OPTIONS`: `--max-old-space-size=1536`

### S3 트리거 설정

- Event type: `s3:ObjectCreated:*`
- Prefix: `original/`
- Suffix: `.jpg`, `.jpeg`, `.png`, `.webp`

## 생성되는 이미지들

업로드된 `original/photo.jpg`에 대해 다음 파일들이 생성됩니다:

- `optimized/photo.jpg` - 최적화된 이미지 (최대 1200px, 비율 유지)
- `thumbnails/photo.jpg` - 썸네일 (최대 300px, 비율 유지)
- `sizes/photo-small.jpg` - 320px 너비 (모바일용)
- `sizes/photo-medium.jpg` - 640px 너비 (태블릿용)
- `sizes/photo-large.jpg` - 1024px 너비 (데스크톱용)
- `webp/photo.webp` - WebP 형식

### 프론트엔드 (반응형 이미지 사용)

```html
<!-- 반응형 이미지 -->
<picture>
  <source srcset="https://cdn.example.com/webp/photo.webp" type="image/webp" />
  <source
    srcset="
      https://cdn.example.com/sizes/photo-small.jpg   400w,
      https://cdn.example.com/sizes/photo-medium.jpg  800w,
      https://cdn.example.com/sizes/photo-large.jpg  1200w
    "
    sizes="(max-width: 400px) 400px, (max-width: 800px) 800px, 1200px"
  />
  <img src="https://cdn.example.com/optimized/photo.jpg" alt="Photo" />
</picture>

<!-- 썸네일 (비율 유지) -->
<img src="https://cdn.example.com/thumbnails/photo.jpg" alt="Thumbnail" />

<!-- 정사각형 썸네일 (프로필 이미지 등) -->
<img
  src="https://cdn.example.com/square-thumbnails/photo.jpg"
  alt="Square Thumbnail"
/>
```

### JavaScript (동적 이미지 로딩)

```javascript
const getImageUrl = (filename, type = "optimized", size = null) => {
  const baseUrl = "https://your-cloudfront-domain.com";

  switch (type) {
    case "thumbnail":
      return `${baseUrl}/thumbnails/${filename}`;
    case "square-thumbnail":
      return `${baseUrl}/square-thumbnails/${filename}`;
    case "webp":
      return `${baseUrl}/webp/${filename.replace(/\.[^.]+$/, ".webp")}`;
    case "size":
      const sizeFilename = filename.replace(/\.([^.]+)$/, `-${size}.$1`);
      return `${baseUrl}/sizes/${sizeFilename}`;
    default:
      return `${baseUrl}/optimized/${filename}`;
  }
};

// 사용 예시
const thumbnailUrl = getImageUrl("photo.jpg", "thumbnail"); // 비율 유지 썸네일
const squareThumbnailUrl = getImageUrl("photo.jpg", "square-thumbnail"); // 정사각형 썸네일
const webpUrl = getImageUrl("photo.jpg", "webp");
const mediumUrl = getImageUrl("photo.jpg", "size", "medium");
```
