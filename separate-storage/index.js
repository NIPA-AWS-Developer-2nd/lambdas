const AWS = require("aws-sdk");
const sharp = require("sharp");

const s3 = new AWS.S3();

exports.handler = async (event) => {
  console.log("Event:", JSON.stringify(event, null, 2));

  let records = [];

  // 이벤트 형태 판별 및 처리
  if (event.detail && event.detail.bucket && event.detail.object) {
    // EventBridge 이벤트
    console.log("Processing EventBridge event");
    records = [
      {
        s3: {
          bucket: { name: event.detail.bucket.name },
          object: { key: event.detail.object.key },
        },
      },
    ];
  } else if (
    event.Records &&
    Array.isArray(event.Records) &&
    event.Records.length > 0
  ) {
    // S3 직접 트리거 이벤트
    console.log("Processing S3 direct trigger event");
    records = event.Records;
  } else {
    // 테스트 이벤트 또는 잘못된 형태
    console.log("No valid S3 or EventBridge event found");

    if (event.test === true) {
      return {
        statusCode: 200,
        body: JSON.stringify({
          message:
            "Lambda function is working. Configure S3 trigger or EventBridge to process images.",
          note: "This function processes images when triggered by S3 events in the 'original/' folder.",
        }),
      };
    }

    return {
      statusCode: 400,
      body: JSON.stringify({
        error: "Invalid event format. Expected S3 event or EventBridge event.",
        received: Object.keys(event),
      }),
    };
  }

  if (records.length === 0) {
    console.log("No records to process");
    return {
      statusCode: 200,
      body: JSON.stringify({ message: "No records to process" }),
    };
  }

  for (const record of records) {
    const bucket = record.s3.bucket.name;
    const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));

    // original/ 폴더의 파일만 처리
    if (!key.startsWith("original/")) {
      console.log("Skipping non-original file:", key);
      continue;
    }

    try {
      console.log(`Processing file: ${key}`);

      // S3에서 원본 이미지 가져오기
      const originalImage = await s3
        .getObject({
          Bucket: bucket,
          Key: key,
        })
        .promise();

      const filename = key.replace("original/", "");
      const fileExtension = filename.split(".").pop().toLowerCase();

      // 이미지 형식 확인 (확장자 기반)
      const supportedImageFormats = [
        "jpg",
        "jpeg",
        "png",
        "webp",
        "gif",
        "bmp",
        "tiff",
      ];
      if (!supportedImageFormats.includes(fileExtension)) {
        console.log("Unsupported file format:", fileExtension);
        continue;
      }

      // MIME 타입으로 추가 검증
      const contentType = originalImage.ContentType || "";
      if (!contentType.startsWith("image/")) {
        console.log("Not an image file based on Content-Type:", contentType);
        continue;
      }

      // 이미지 최적화
      const optimizedBuffer = await optimizeImage(
        originalImage.Body,
        fileExtension
      );

      // 썸네일 생성 (비율 유지)
      const thumbnailBuffer = await createThumbnail(
        originalImage.Body,
        fileExtension
      );

      // 여러 크기 버전 생성 (반응형 이미지용, 비율 유지)
      const sizes = [
        { name: "small", width: 320 }, // 모바일용
        { name: "medium", width: 640 }, // 태블릿용
        { name: "large", width: 1024 }, // 데스크톱용
      ];

      const resizedImages = await Promise.all(
        sizes.map(async (size) => ({
          name: size.name,
          buffer: await createResizedImage(
            originalImage.Body,
            fileExtension,
            size.width
          ),
        }))
      );

      // 파일 크기 정보
      const originalSize = originalImage.Body.length;
      const optimizedSize = optimizedBuffer.length;
      const compressionRatio = (
        ((originalSize - optimizedSize) / originalSize) *
        100
      ).toFixed(2);

      console.log(
        `Size reduction: ${originalSize} -> ${optimizedSize} bytes (${compressionRatio}% saved)`
      );

      // 최적화된 이미지 저장
      await s3
        .putObject({
          Bucket: bucket,
          Key: `optimized/${filename}`,
          Body: optimizedBuffer,
          ContentType:
            originalImage.ContentType ||
            `image/${fileExtension === "jpg" ? "jpeg" : fileExtension}`,
          CacheControl: "max-age=31536000", // 1년 캐시
          Metadata: {
            originalSize: originalSize.toString(),
            optimizedSize: optimizedSize.toString(),
            compressionRatio: compressionRatio,
            optimizedAt: new Date().toISOString(),
          },
        })
        .promise();

      // 썸네일 저장 (비율 유지)
      await s3
        .putObject({
          Bucket: bucket,
          Key: `thumbnails/${filename}`,
          Body: thumbnailBuffer,
          ContentType: `image/${
            fileExtension === "jpg" ? "jpeg" : fileExtension
          }`,
          CacheControl: "max-age=31536000",
        })
        .promise();

      // 여러 크기 버전 저장
      await Promise.all(
        resizedImages.map(async (resized) => {
          const sizeFilename = filename.replace(
            /\.([^.]+)$/,
            `-${resized.name}.$1`
          );
          return s3
            .putObject({
              Bucket: bucket,
              Key: `sizes/${sizeFilename}`,
              Body: resized.buffer,
              ContentType: `image/${
                fileExtension === "jpg" ? "jpeg" : fileExtension
              }`,
              CacheControl: "max-age=31536000",
            })
            .promise();
        })
      );

      // WebP 버전 생성 (모던 브라우저용)
      const webpBuffer = await convertToWebP(originalImage.Body);
      const webpFilename = filename.replace(/\.[^.]+$/, ".webp");

      await s3
        .putObject({
          Bucket: bucket,
          Key: `webp/${webpFilename}`,
          Body: webpBuffer,
          ContentType: "image/webp",
          CacheControl: "max-age=31536000",
        })
        .promise();

      console.log(`Successfully processed: ${key}`);
      console.log(`Generated files:`);
      console.log(`- optimized/${filename}`);
      console.log(`- thumbnails/${filename} (비율 유지)`);
      console.log(`- webp/${webpFilename}`);
      resizedImages.forEach((resized) => {
        const sizeFilename = filename.replace(
          /\.([^.]+)$/,
          `-${resized.name}.$1`
        );
        console.log(`- sizes/${sizeFilename}`);
      });
    } catch (error) {
      console.error(`Error processing ${key}:`, error);
      throw error;
    }
  }

  return {
    statusCode: 200,
    body: JSON.stringify({ message: "Images processed successfully" }),
  };
};

async function optimizeImage(buffer, extension) {
  let sharpInstance = sharp(buffer);

  // 이미지 메타데이터 가져오기
  const metadata = await sharpInstance.metadata();
  console.log(
    `Original image: ${metadata.width}x${metadata.height}, ${metadata.format}, ${buffer.length} bytes`
  );

  // EXIF 회전 정보 적용
  sharpInstance = sharpInstance.rotate();

  // 큰 이미지는 리사이징 (최대 1200px, 비율 유지)
  const maxDimension = 1200;
  if (metadata.width > maxDimension || metadata.height > maxDimension) {
    sharpInstance = sharpInstance.resize(maxDimension, maxDimension, {
      fit: "inside", // 비율 유지하면서 최대 크기 내에 맞춤
      withoutEnlargement: true, // 원본보다 크게 만들지 않음
    });
    console.log(
      `Resizing from ${metadata.width}x${metadata.height} to max ${maxDimension}px`
    );
  }

  switch (extension) {
    case "jpg":
    case "jpeg":
      return await sharpInstance
        .jpeg({
          quality: 85,
          progressive: true,
          mozjpeg: true,
        })
        .toBuffer();

    case "png":
      return await sharpInstance
        .png({
          quality: 85,
          compressionLevel: 9,
        })
        .toBuffer();

    case "webp":
      return await sharpInstance
        .webp({
          quality: 85,
          effort: 6,
        })
        .toBuffer();

    default:
      throw new Error(`Unsupported image format: ${extension}`);
  }
}

async function createThumbnail(buffer, extension) {
  const format = extension === "jpg" ? "jpeg" : extension;

  // 비율 유지 썸네일 (최대 300px)
  return await sharp(buffer)
    .resize(300, 300, {
      fit: "inside", // 비율 유지하면서 300x300 내에 맞춤
      withoutEnlargement: true,
    })
    .toFormat(format, { quality: 80 })
    .toBuffer();
}

async function createResizedImage(buffer, extension, width) {
  const format = extension === "jpg" ? "jpeg" : extension;

  return await sharp(buffer)
    .resize(width, null, {
      fit: "inside",
      withoutEnlargement: true,
    })
    .toFormat(format, { quality: 85 })
    .toBuffer();
}

async function convertToWebP(buffer) {
  return await sharp(buffer)
    .webp({
      quality: 85,
      effort: 6,
    })
    .toBuffer();
}
