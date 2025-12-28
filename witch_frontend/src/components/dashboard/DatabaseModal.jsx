import { useState } from 'react';

export function DatabaseModal({ isOpen, onClose, onConnect, isConnecting = false }) {
  const [formData, setFormData] = useState({
    host: '',
    port: '5432',
    database: '',
    user: '',
    password: '',
    db_type: 'postgres',
  });

  const [errors, setErrors] = useState({});

  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData((prev) => ({ ...prev, [name]: value }));
    if (errors[name]) {
      setErrors((prev) => ({ ...prev, [name]: null }));
    }
  };

  const validate = () => {
    const newErrors = {};
    if (!formData.host.trim()) newErrors.host = 'REQUIRED';
    if (!formData.port.trim()) newErrors.port = 'REQUIRED';
    if (!formData.database.trim()) newErrors.database = 'REQUIRED';
    if (!formData.user.trim()) newErrors.user = 'REQUIRED';
    if (!formData.password.trim()) newErrors.password = 'REQUIRED';
    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    if (!validate()) return;

    onConnect({
      host: formData.host.trim(),
      port: parseInt(formData.port, 10),
      database: formData.database.trim(),
      user: formData.user.trim(),
      password: formData.password,
      db_type: formData.db_type,
    });
  };

  const handleBackdropClick = (e) => {
    if (e.target === e.currentTarget && !isConnecting) {
      onClose();
    }
  };

  if (!isOpen) return null;

  return (
    <div
      onClick={handleBackdropClick}
      className="fixed inset-0 bg-black/80 flex items-center justify-center z-[100]"
    >
      <div className="w-full max-w-lg bg-[#0a0a0a] border border-[#2a2a2a]">
        {/* Header */}
        <div className="flex items-center justify-between terminal-header">
          <span>◆ DATABASE CONNECTION</span>
          <button
            type="button"
            onClick={onClose}
            disabled={isConnecting}
            className="text-black hover:bg-[#cc5500] px-2 py-0.5 text-[10px] font-bold disabled:opacity-50"
          >
            [X] CLOSE
          </button>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit} className="p-4">
          {/* Database Type */}
          <div className="mb-4">
            <label className="block text-[10px] text-[#606060] uppercase tracking-wide mb-2">
              DATABASE TYPE
            </label>
            <div className="flex gap-2">
              {['postgres', 'mysql'].map((type) => (
                <button
                  key={type}
                  type="button"
                  onClick={() => setFormData((prev) => ({ ...prev, db_type: type }))}
                  className={`flex-1 py-2 text-xs uppercase tracking-wide ${
                    formData.db_type === type
                      ? 'btn-terminal-primary'
                      : 'btn-terminal'
                  }`}
                >
                  {type === 'postgres' ? 'POSTGRESQL' : 'MYSQL'}
                </button>
              ))}
            </div>
          </div>

          {/* Host & Port */}
          <div className="grid grid-cols-3 gap-3 mb-4">
            <div className="col-span-2">
              <label className="block text-[10px] text-[#606060] uppercase tracking-wide mb-2">
                HOST {errors.host && <span className="text-[#ff1744]">({errors.host})</span>}
              </label>
              <input
                type="text"
                name="host"
                value={formData.host}
                onChange={handleChange}
                placeholder="localhost"
                disabled={isConnecting}
                className={`input-terminal w-full ${errors.host ? 'border-[#ff1744]' : ''}`}
              />
            </div>
            <div>
              <label className="block text-[10px] text-[#606060] uppercase tracking-wide mb-2">
                PORT {errors.port && <span className="text-[#ff1744]">({errors.port})</span>}
              </label>
              <input
                type="text"
                name="port"
                value={formData.port}
                onChange={handleChange}
                placeholder="5432"
                disabled={isConnecting}
                className={`input-terminal w-full ${errors.port ? 'border-[#ff1744]' : ''}`}
              />
            </div>
          </div>

          {/* Database Name */}
          <div className="mb-4">
            <label className="block text-[10px] text-[#606060] uppercase tracking-wide mb-2">
              DATABASE {errors.database && <span className="text-[#ff1744]">({errors.database})</span>}
            </label>
            <input
              type="text"
              name="database"
              value={formData.database}
              onChange={handleChange}
              placeholder="my_database"
              disabled={isConnecting}
              className={`input-terminal w-full ${errors.database ? 'border-[#ff1744]' : ''}`}
            />
          </div>

          {/* Username & Password */}
          <div className="grid grid-cols-2 gap-3 mb-6">
            <div>
              <label className="block text-[10px] text-[#606060] uppercase tracking-wide mb-2">
                USERNAME {errors.user && <span className="text-[#ff1744]">({errors.user})</span>}
              </label>
              <input
                type="text"
                name="user"
                value={formData.user}
                onChange={handleChange}
                placeholder="postgres"
                disabled={isConnecting}
                className={`input-terminal w-full ${errors.user ? 'border-[#ff1744]' : ''}`}
              />
            </div>
            <div>
              <label className="block text-[10px] text-[#606060] uppercase tracking-wide mb-2">
                PASSWORD {errors.password && <span className="text-[#ff1744]">({errors.password})</span>}
              </label>
              <input
                type="password"
                name="password"
                value={formData.password}
                onChange={handleChange}
                placeholder="••••••••"
                disabled={isConnecting}
                className={`input-terminal w-full ${errors.password ? 'border-[#ff1744]' : ''}`}
              />
            </div>
          </div>

          {/* Actions */}
          <div className="flex gap-3 border-t border-[#2a2a2a] pt-4">
            <button
              type="button"
              onClick={onClose}
              disabled={isConnecting}
              className="btn-terminal flex-1 py-3"
            >
              [ESC] CANCEL
            </button>
            <button
              type="submit"
              disabled={isConnecting}
              className="btn-terminal-primary flex-1 py-3"
            >
              {isConnecting ? 'CONNECTING...' : '[ENTER] CONNECT'}
            </button>
          </div>
        </form>

        {/* Footer hint */}
        <div className="px-4 py-2 bg-[#121212] border-t border-[#2a2a2a] text-[11px] text-[#909090]">
          <span className="text-[#606060]">CONNECTION STRING:</span> {formData.db_type}://{formData.user || 'user'}@{formData.host || 'host'}:{formData.port}/{formData.database || 'db'}
        </div>
      </div>
    </div>
  );
}
