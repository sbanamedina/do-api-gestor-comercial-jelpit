from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
import sqlalchemy
import os

from connect_sql import get_db
from models import NitRequest

app = FastAPI()
puerto = os.environ.get("PORT", 8080)

# =========
# ENDPOINTS
# =========

# Información de conjuntos por NIT
@app.post("/bi_info_conjuntos")
def get_bi_info_conjuntos(req: NitRequest, db: Session = Depends(get_db)):
    query = sqlalchemy.text("""
        SELECT
            "NIT_SIN_DIGITO_VERIFICACION",
            "NIT",
            "REPRESENTANTE_LEGAL_ID",
            "NOMBRE_CLIENTE",
            "CELULAR_CLIENTE",
            "TELEFONO_CLIENTE",
            "CORREO_ELECTRONICO",
            "CORREO_ELECTRONICO_PERSONA_JURIDICA",
            "FLG_CORREO_ELECTRONICO",
            "FLG_CELULAR_RELACIONADO",
            "FLG_TELEFONO_RELACIONADO",
            "CLIENTE_EMPRESARIAL",
            "TIPO_CUENTA",
            "NRO_CUENTA",
            "TIPO_RELACION",
            "LISTAS_RESTRICTIVAS",
            "COD_AREA_REPORTE",
            "AREA_REPORTE",
            "LISTA_RESTRICTIVAS_RL",
            "CLIENTE_BANCO",
            "CLIENTE_PORTAL_ACTIVO",
            "SEGMENTO_CLIENTE",
            "TIENE_CUENTA_AH",
            "TIENE_CUENTA_CTE",
            "TIENE_CONVENIO_RECAUDO",
            "TIENE_ADQUIRENCIAS",
            "MONTO_SALDO_PROM",
            "PERIODO",
            "DESCRIPCION_COMBO",
            "NUMERO_ACUERDO",
            "MERCHAN_ID",
            "ES_CLIENTE_CONJUNTOS"
        FROM "api_backend"."bi_info_conjuntos"
        WHERE "NIT" = :nit
        ORDER BY "PERIODO" DESC;
    """)
    results = db.execute(query, {"nit": req.nit}).fetchall()
    if not results:
        raise HTTPException(status_code=404, detail="NIT no encontrado")
    return [dict(row._mapping) for row in results]


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(puerto))



