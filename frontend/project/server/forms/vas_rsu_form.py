from flask_wtf import FlaskForm
from wtforms import FileField, SelectField, SubmitField, IntegerField, FloatField
from wtforms.validators import InputRequired, DataRequired

D_DEFAULT_LINK_LEN  = 8000.0
D_DEFAULT_TX_RATE   = 2000000.0     # 2Mbps
D_SPEED_OF_LIGHT    = 299792458.0   # ~300 * 10^6 m/sec

class VasRSUForm(FlaskForm):
    #number_of_workers = SelectField('Number of RSUs/cluster:', validators=[DataRequired()], id='select_worker_number')
    number_of_workers = IntegerField('Number of RSUs/cluster:', validators=[DataRequired()], id='select_worker_number')
    number_of_masters = SelectField('Number of Clusters:',     validators=[DataRequired()], id='select_master_number')
    submit = SubmitField('Set number of RSU\'s')

class VasPopulate(FlaskForm):
   # number_of_nodes = IntegerField('number of nodes', validators=[Required()])
   rows_of_data = IntegerField('Rows of data:', validators=[DataRequired()])
   # submit = SubmitField('Populate with data')

class VasDeleteDB(FlaskForm):
   submit = SubmitField('Delete all databases')

class VasDelayProfileForm(FlaskForm):
    cluster_tx_rate     = FloatField( 'Tx Rate (bits/sec):',         id='cluster_tx_rate',    default=D_DEFAULT_TX_RATE )
    cluster_prop_speed  = FloatField( 'Propagation Speed (m/sec):',  id='cluster_prop_speed', default=D_SPEED_OF_LIGHT )
    cluster_link_length = FloatField( 'Link Length (m):',            id='cluster_link_len',   default=D_DEFAULT_LINK_LEN)
    cluster_proc_delay  = FloatField( 'Processing Delay (sec):',     id='cluster_proc_delay', default=0.0 )
    cluster_queueing_delay = FloatField( 'Queueing Delay (sec):',    id='cluster_queueing_delay', default=0.0 )
    gateway_tx_rate     = FloatField( 'Tx Rate (bits/sec):',         id='gateway_tx_rate',    default=D_DEFAULT_TX_RATE )
    gateway_prop_speed  = FloatField( 'Propagation Speed (m/sec):',  id='gateway_prop_speed', default=D_SPEED_OF_LIGHT )
    gateway_link_length = FloatField( 'Link Length (m):',            id='gateway_link_len',   default=D_DEFAULT_LINK_LEN * 2.2)
    gateway_proc_delay  = FloatField( 'Processing Delay (sec):',     id='gateway_proc_delay', default=0.0)
    gateway_queueing_delay = FloatField( 'Queueing Delay (sec):',    id='gateway_queueing_delay', default=0.0 )


