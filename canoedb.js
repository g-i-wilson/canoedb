'use strict';


const e = React.createElement;

class RowsTable extends React.Component {
	constructor(props) {
		super(props);
		//this.state = {...props};
	}

	render() {
		
		let headerArray = [];
		let rowsArray = [];
		let headerComplete = false;
		Object.keys(this.props).sort().forEach(row => {
			let rowArray = [];
			Object.keys(this.props[row]).sort().forEach(table => {
				Object.keys(this.props[row][table]).sort().forEach(column => {
					!headerComplete && headerArray.push( column );
					rowArray.push( this.props[row][table][column] );
				});
			});
			headerComplete = true;
			rowsArray.push( rowArray );
		});
		console.log( headerArray );
		console.log( rowsArray );
		
		
		return e(
			'table',
			{},
			// single header row
			e(
				'tbody',
				{},
				e(
					'tr',
					{},
					headerArray.map((header) => {
						return e( 'th', {}, header ) 
					})
				),
				// many data rows
				rowsArray.map((row) => {
					return e(
						'tr',
						{},
						// data strings
						row.map((data) => {
							return e( 'td', {}, data );
						})
					)
				})
			)
		);
	}
}

class ColumnHeader extends React.Component {
	constructor(props) {
		super(props);
		this.state = {...props};

		this.inputChange = this.inputChange.bind(this);
	}

	inputChange(event) {
		// assign actual variables, since by the time setState runs the function passed to...
		// ...it, I found that sometimes the event.target object reference had already become null.
		var targetName = event.target.name;
		var targetValue;
		// check for type
		if ( event.target.type === 'checkbox') {
			var targetValue = event.target.checked;
		} else {
			var targetValue = ( event.target.value ? event.target.value : '' );
		}
		// pass a function to setState
		this.setState(s => {
			s[targetName] = targetValue;
			this.state.update(this.state);
		});
	}
	
	render() {
		if (this.state.reference) {
			// hide columns that reference other tables
			return null;
		} else {
			return e(
				'div',
				{
					className: 'column'
				},
				e(
					'input',
					{
						name: "enabled",
						type: "checkbox",
						checked: this.state.enabled,
						onChange: this.inputChange,
						className: 'enableCheckBox'
					}
				),
				e(
					'div',
					{
						className: ( this.state.enabled ? 'columnTitle highlighed' : 'columnTitle' )
					},
					this.props.column+':'
				),
				e(
					'input',
					{
						name: "filter",
						type: "text",
						value: this.state.filter,
						onChange: this.inputChange,
						className: ( this.state.enabled && this.state.filter ? 'filterInput highlighed' : 'filterInput' )
					}
				),
				e(
					'input',
					{
						name: "transform",
						type: "text",
						value: this.state.transform,
						onChange: this.inputChange,
						className: ( this.state.enabled && this.state.transform && this.state.filter ? 'transformInput highlighed' : 'transformInput' )
					}
				)
			);
		}
	}
}

class CanoeDB extends React.Component {
	constructor(props) {
		super(props);
		this.state = {
			error: null,
			isLoaded: false,
			settings: {},
			structure: {},
			columns: {},
			rows: {}
		}
		
		this.update = this.update.bind(this);
		this.transmit = this.transmit.bind(this);
	}
	
	update(newSettings = {}) {
		let table = newSettings.table;
		let column = newSettings.column;
		this.setState(s => {
			// vivify if necessary
			if (!s.settings.hasOwnProperty(table)) s.settings[table] = {};
			if (!s.settings[table].hasOwnProperty(column)) s.settings[table][column] = {};
			// overlay newSettings onto settings
			Object.assign( s.settings[table][column], newSettings );
			// trigger a new transmission
			this.transmit();
		});
	}
	
	transmit( writeMode ) {
		// start new transmission
		let settings = this.state.settings;
		console.log(settings);
		
		let query = [];
		Object.keys(settings).sort().forEach(table => {
			Object.keys(settings[table]).sort().forEach(column => {
				let thisCol = settings[table][column];
				if (thisCol.enabled) {
					query.push( table+'.'+column+( thisCol.transform ? '.'+thisCol.transform : '' )+'='+thisCol.filter );
				}
			});
		});
		var url = "http://localhost:8091/json"+
			( writeMode ? '/write' : '' )+
			( query.length>0 ? '?'+query.join('&') : '' );
		console.log('GET '+url);
		fetch( url, {
			// mode: 'no-cors' // 'cors' by default
			mode: 'cors'
		})
		.then(res => res.json())
		.then(
			(result) => {
				this.setState({
					isLoaded: true,
					structure: result.structure,
					columns: result.columns,
					rows: result.rows
				});
			},
			// Note: it's important to handle errors here
			// instead of a catch() block so that we don't swallow
			// exceptions from actual bugs in components.
			(error) => {
				this.setState({
					isLoaded: true,
					error
				});
			}
		)
	}
	
	componentDidMount() {
		// blank transmission to initialize
		this.transmit();
	}
	
	render() {
		
		console.log("Rendering Interface...");		
		
		const { error, isLoaded, structure, rows, columns, settings } = this.state;
		if (error) {
			return e(
				'div',
				{},
				e( 'p', {}, error.message ),
				e( 'p', {}, JSON.stringify( structure ) )
			);
		} else if (!isLoaded) {
			return e(
				'div',
				{},
				'Loading...'
			);
		} else {
			return e(
				'div',
				{
					className: 'all'
				},
				// banner DIV
				e(
					'div',
					{
						className: 'banner'
					},
					e( 'p', {className: 'insignia'}, 'CanoeDB' )
				),
				e(
					// header DIV
					'div',
					{
						className: 'header'
					},
					// loop through tables
					Object.keys(structure).sort().map((table) => {
						// table DIV
						return e(
							'div',
							{
								key: table,
								className: 'dbTable'
							},
							e( 'div', {className: 'tableName'}, table ),
							// loop through columns
							e(
								'div',
								{
									className: 'tableEnvelope'
								},
								Object.keys(structure[table]).sort().map((column) => {
									let props_obj = Object.assign(
										// default values
										{
											key: table+column,
											table: table,
											column: column,
											filter: '',
											transform: '',
											reference: '',
											enabled: false,
											update: this.update
										},
										// structure returned from the database
										structure[table][column],
										// any settings produced by the interface
										(
											settings.hasOwnProperty(table) && settings[table].hasOwnProperty(column) ?
											settings[table][column] : {}
										)
									);
									// column Element
									return e( ColumnHeader, props_obj );
								})
							)
						);
					})
				),
				e(
					// build rows display
					'div',
					{
						className: 'rows'
					},
					//e( 'p', {}, JSON.stringify( rows ) )
					e( RowsTable, this.state.rows )
				)
			);
		}
	}
}


const domContainer = document.querySelector('#root');
ReactDOM.render(e(CanoeDB), domContainer);

