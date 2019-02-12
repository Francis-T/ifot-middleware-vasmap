from flask_wtf import FlaskForm
from wtforms import FileField, SelectField, SubmitField, IntegerField
from wtforms.validators import InputRequired, DataRequired

class VasRSUForm(FlaskForm):
    number_of_workers = SelectField('Number of workers:', validators=[DataRequired()], id='select_worker_number')
    number_of_masters = SelectField('Number of master:', validators=[DataRequired()], id='select_master_number')
    submit = SubmitField('Set number of RSU\'s')

class VasPopulate(FlaskForm):
   # number_of_nodes = IntegerField('number of nodes', validators=[Required()])
   rows_of_data = IntegerField('Rows of data:', validators=[DataRequired()])
   # submit = SubmitField('Populate with data')

class VasDeleteDB(FlaskForm):
   submit = SubmitField('Delete all databases')