resource "aws_s3_bucket" "social_analysis" {
    bucket = "social-analysis-${var.environment}-bucket"

    tags = {
        Name        = "social-analysis-${var.environment}-bucket"
        Environment = var.environment
        Project     = "social-media-analysis-platform"
    }
}

resource "aws_s3_bucket_versioning" "social_analysis" {
    bucket = aws_s3_bucket.social_analysis.id

    versioning_configuration {
        status = "Enabled"
    }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "social_analysis" {
    bucket = aws_s3_bucket.social_analysis.id
    
    rule {
        apply_server_side_encryption_by_default {
            sse_algorithm = "AES256"
        }
    }
}

resource "aws_s3_bucket_public_access_block" "social_analysis" {
    bucket = aws_s3_bucket.social_analysis.id

    block_public_acls       = true
    block_public_policy     = true
    ignore_public_acls      = true
    restrict_public_buckets = true 
}

resource "aws_s3_bucket_lifecycle_configuration" "social_analysis" {
    bucket = aws_s3_bucket.social_analysis.id

    rule {
        id     = "transition-to-ia"
        status = "Enabled"

        transition {

            days          = 30
            storage_class = "STANDARD_IA"
        }

        transition {
            days          = 90
            storage_class = "GLACIER"
        }
    }
}