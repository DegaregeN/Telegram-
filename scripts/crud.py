from sqlalchemy.orm import Session
import models, schemas

def get_detection_result(db: Session, detection_id: int):
    return db.query(models.DetectionResult).filter(models.DetectionResult.id == detection_id).first()

def get_detection_results(db: Session, skip: int = 0):
    return db.query(models.DetectionResult).offset(skip).all()

def create_detection_result(db: Session, detection: schemas.DetectionResultCreate):
    db_detection = models.DetectionResult(**detection.dict())
    db.add(db_detection)
    db.commit()
    db.refresh(db_detection)
    return db_detection

def update_detection_result(db: Session, detection_id: int, detection: schemas.DetectionResultCreate):
    db_detection = get_detection_result(db, detection_id)
    if db_detection is None:
        return None
    for key, value in detection.dict().items():
        setattr(db_detection, key, value)
    db.commit()
    db.refresh(db_detection)
    return db_detection

def delete_detection_result(db: Session, detection_id: int):
    db_detection = get_detection_result(db, detection_id)
    if db_detection is None:
        return None
    db.delete(db_detection)
    db.commit()
    return db_detection