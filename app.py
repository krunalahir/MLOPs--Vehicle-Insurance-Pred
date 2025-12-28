from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.responses import HTMLResponse, RedirectResponse
from uvicorn import run as app_run

from typing import Optional

# Importing constants and pipeline modules from the project
from src.constants import APP_HOST,APP_PORT
from src.pipline.prediction_pipeline import VehicleData, VehicleDataClassifier
from src.pipline.training_pipeline import TrainPipeline

#initialize Fastapi application
app =FastAPI()

# Mount the 'static' directory for serving static files (like CSS)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Set up Jinja2 template engine for rendering HTML templates
templates = Jinja2Templates(directory='templates')

# Allow all origins for Cross-Origin Resource Sharing (CORS)
origins = ["*"]

# Configure middleware to handle CORS, allowing requests from any origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

#Route to render the main page with the form
@app.get("/", tags=["authentication"])
async def index(request:Request):
    """
    Render the main HTML page for vehicle data inputs.
    """
    return templates.TemplateResponse(
        "vehicledata.html",{"request":request,"context":"Please fill in the form"})

# Route to trigger the model training process
@app.get("/train")
async def trainRouteClient():
    """
    Endpoint to initiate the training pipeline.
    """
    try:
        train_pipeline=TrainPipeline()
        train_pipeline.run_pipeline()
        return Response("Training successfull!!")

    except Exception as e:
        return Response(f"error occured: {e}")

class DataForm:
    """
        DataForm class to handle and process incoming form data.
        This class defines the vehicle-related attributes expected from the form.
    """
    def __init__(self, request: Request):
        self.request: Request = request
        self.Gender: Optional[int] = None
        self.Age: Optional[int] = None
        self.Driving_License: Optional[int] = None
        self.Region_Code: Optional[float] = None
        self.Previously_Insured: Optional[int] = None
        self.Annual_Premium: Optional[float] = None
        self.Policy_Sales_Channel: Optional[float] = None
        self.Vintage: Optional[int] = None
        self.Vehicle_Age_lt_1_Years: Optional[int] = None  # Changed to match model expectation
        self.Vehicle_Age_gt_2_Years: Optional[int] = None
        self.Vehicle_Damage_Yes: Optional[int] = None

    async def get_vehicle_data(self):
        """
        Method to retrieve and assign form data to class attributes.
        This method is asynchronous to handle form data fetching without blocking.
        """
        form = await self.request.form()
        self.Gender = form.get("Gender")
        self.Age = form.get("Age")
        self.Driving_License = form.get("Driving_License")
        self.Region_Code = form.get("Region_Code")
        self.Previously_Insured = form.get("Previously_Insured")
        self.Annual_Premium = form.get("Annual_Premium")
        self.Policy_Sales_Channel = form.get("Policy_Sales_Channel")
        self.Vintage = form.get("Vintage")
        # Map the form field to the expected model column name
        self.Vehicle_Age_lt_1_Years = form.get("Vehicle_Age_lt_1_Years")  # Map to correct name (with 's')
        self.Vehicle_Age_gt_2_Years = form.get("Vehicle_Age_gt_2_Years")
        self.Vehicle_Damage_Yes = form.get("Vehicle_Damage_Yes")

#Route to handle the form and make the prediction
@app.post("/")
async def predictRouteClient(request:Request):
    """
    Endpoint to recieve form data ,process it and make a prediction
    """
    try:
        form=DataForm(request)
        await form.get_vehicle_data()

        vehicle_data = VehicleData(
            Gender=form.Gender,
            Age=form.Age,
            Driving_License=form.Driving_License,
            Region_Code=form.Region_Code,
            Previously_Insured=form.Previously_Insured,
            Annual_Premium=form.Annual_Premium,
            Policy_Sales_Channel=form.Policy_Sales_Channel,
            Vintage=form.Vintage,
            Vehicle_Age_lt_1_Years=form.Vehicle_Age_lt_1_Years,  # Use the corrected field name
            Vehicle_Age_gt_2_Years=form.Vehicle_Age_gt_2_Years,
            Vehicle_Damage_Yes=form.Vehicle_Damage_Yes
        )

        #convert the form data to dataframe for the model
        vehicle_df=vehicle_data.get_vehicle_input_data_frame()

        # Add the missing 'id' column with a default value
        vehicle_df['id'] = 0  # Add the missing 'id' column

        #initialize the prediction pipeline
        model_predictor=VehicleDataClassifier()

        #make a prediction and retrieve the result
        value=model_predictor.predict(dataframe=vehicle_df)

        #interpret the prediction result as response yes or no
        status="Response-Yes" if value==1 else "Response-No"

        #render the same html page with the prediction result
        return templates.TemplateResponse(
            "vehicledata.html",
            {"request":request,"context":status}
        )

    except Exception as e:
        return {"status": False, "message": str(e)}

# Main entry point to start the FastAPI server
if __name__ == "__main__":
    app_run(app, host=APP_HOST, port=APP_PORT)

